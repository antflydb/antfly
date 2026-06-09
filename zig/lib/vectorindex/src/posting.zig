// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const vector_store = @import("store.zig");
const types = @import("types.zig");
const vec = @import("antfly_vector").vector;

pub const VectorId = u64;
pub const PostingId = u64;

pub const PostingView = struct {
    id: PostingId,
    parent: PostingId,
    level: u16,
    centroid: []const f32,
    members: []const VectorId,
    state: types.PostingState,

    pub fn usesNonQuantizedPayload(self: PostingView) bool {
        return self.parent == 0;
    }

    pub fn hasFreshStoredPayload(self: PostingView) bool {
        return !self.state.payload_dirty;
    }
};

pub const PostingState = types.PostingState;

pub const PostingBase = struct {
    posting_id: PostingId,
    generation: u64,
    members: []const VectorId,
};

pub const OwnedPostingBase = struct {
    posting_id: PostingId,
    generation: u64,
    members: []VectorId,

    pub fn deinit(self: *OwnedPostingBase, alloc: std.mem.Allocator) void {
        alloc.free(self.members);
        self.* = .{
            .posting_id = 0,
            .generation = 0,
            .members = &.{},
        };
    }
};

pub const PostingDeltaOp = enum(u8) {
    insert = 1,
    tombstone = 2,
    replace = 3,
};

pub const PostingDeltaRecord = struct {
    sequence: u64,
    op: PostingDeltaOp,
    vector_id: VectorId,
};

pub const FoldDeltaTailResult = struct {
    delta_records: usize = 0,
    base_member_count: usize = 0,
    materialized_member_count: usize = 0,
    deleted_tail_keys: usize = 0,
    deleted_tail_key_bytes: usize = 0,
    deleted_tail_value_bytes: usize = 0,
    written_base_key_bytes: usize = 0,
    written_base_value_bytes: usize = 0,
    next_generation: u64 = 0,
    skipped: bool = false,
};

const DeleteDeltaTailStats = struct {
    keys: usize = 0,
    key_bytes: usize = 0,
    value_bytes: usize = 0,
};

pub const FoldDeltaTailOptions = struct {
    min_delta_records: usize = 1,
    min_tombstone_records: usize = 0,
    min_delta_to_base_ratio_bps: u32 = 0,
};

pub const CentroidDirectoryRecord = struct {
    posting_id: PostingId,
    generation: u64,
    mutation_version: u64 = 0,
    payload_version: u64 = 0,
    flags: u8 = 0,
    parent: PostingId,
    level: u16,
    member_count: u64,
    bounds_radius: f32 = 0,
    centroid: []const f32,
};

pub const OwnedCentroidDirectoryRecord = struct {
    posting_id: PostingId,
    generation: u64,
    mutation_version: u64,
    payload_version: u64,
    flags: u8,
    parent: PostingId,
    level: u16,
    member_count: u64,
    bounds_radius: f32,
    centroid: []f32,

    pub fn deinit(self: *OwnedCentroidDirectoryRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.centroid);
        self.* = .{
            .posting_id = 0,
            .generation = 0,
            .mutation_version = 0,
            .payload_version = 0,
            .flags = 0,
            .parent = 0,
            .level = 0,
            .member_count = 0,
            .bounds_radius = 0,
            .centroid = &.{},
        };
    }
};

pub const AssignmentRecord = struct {
    vector_id: VectorId,
    posting_id: PostingId,
    version: u64,
    vector_ref: u64,
    flags: u8 = 0,
};

pub const AssignmentFormat = struct {
    pub const magic = [_]u8{ 'A', 'M' };
    pub const version: u8 = 1;
    pub const current_flag: u8 = 1;

    pub const encoded_size: usize = 2 + 1 + 1 + 8 + 4;

    pub fn encode(record: AssignmentRecord, buf: *[encoded_size]u8) []u8 {
        @memcpy(buf[0..2], &magic);
        buf[2] = version;
        buf[3] = record.flags;
        std.mem.writeInt(u64, buf[4..12], record.posting_id, .little);
        std.mem.writeInt(u32, buf[12..16], @intCast(@min(record.version, std.math.maxInt(u32))), .little);
        return buf[0..encoded_size];
    }

    pub fn decode(data: []const u8) !AssignmentRecord {
        if (data.len != encoded_size) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..2], &magic)) return error.BadAssignmentMagic;
        if (data[2] != version) return error.UnsupportedAssignmentVersion;
        if (data[3] != current_flag) return error.UnsupportedAssignmentFlags;
        return .{
            .flags = data[3],
            .vector_id = 0,
            .posting_id = std.mem.readInt(u64, data[4..12], .little),
            .version = std.mem.readInt(u32, data[12..16], .little),
            .vector_ref = 0,
        };
    }
};

pub const CentroidDirectoryFormat = struct {
    pub const magic = [_]u8{ 'A', 'F', 'C', 'D' };
    pub const version: u8 = 1;
    pub const dirty_flag: u8 = 1;
    pub const centroid_dirty_flag: u8 = 2;
    pub const payload_dirty_flag: u8 = 4;

    const header_size: usize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4;

    pub fn encode(alloc: std.mem.Allocator, record: CentroidDirectoryRecord) ![]u8 {
        if (record.centroid.len > std.math.maxInt(u32)) return error.TooLarge;
        const encoded_len = header_size + record.centroid.len * @sizeOf(u32);
        var out = try alloc.alloc(u8, encoded_len);
        errdefer alloc.free(out);

        @memcpy(out[0..4], &magic);
        out[4] = version;
        out[5] = record.flags;
        std.mem.writeInt(u16, out[6..8], record.level, .little);
        std.mem.writeInt(u64, out[8..16], record.posting_id, .little);
        std.mem.writeInt(u64, out[16..24], record.generation, .little);
        std.mem.writeInt(u64, out[24..32], record.mutation_version, .little);
        std.mem.writeInt(u64, out[32..40], record.payload_version, .little);
        std.mem.writeInt(u64, out[40..48], record.parent, .little);
        std.mem.writeInt(u64, out[48..56], record.member_count, .little);
        std.mem.writeInt(u32, out[56..60], @bitCast(record.bounds_radius), .little);
        std.mem.writeInt(u32, out[60..64], @intCast(record.centroid.len), .little);

        var pos: usize = header_size;
        for (record.centroid) |component| {
            std.mem.writeInt(u32, out[pos..][0..4], @bitCast(component), .little);
            pos += 4;
        }
        return out;
    }

    pub fn decode(alloc: std.mem.Allocator, data: []const u8) !OwnedCentroidDirectoryRecord {
        if (data.len < header_size) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..4], &magic)) return error.BadCentroidDirectoryMagic;
        if (data[4] != version) return error.UnsupportedCentroidDirectoryVersion;
        const supported_flags = dirty_flag | centroid_dirty_flag | payload_dirty_flag;
        if (data[5] & ~supported_flags != 0) return error.UnsupportedCentroidDirectoryFlags;
        const centroid_len = std.mem.readInt(u32, data[60..64], .little);
        const expected_len = header_size + @as(usize, centroid_len) * @sizeOf(u32);
        if (data.len != expected_len) return error.Corrupted;

        const centroid = try alloc.alloc(f32, centroid_len);
        errdefer alloc.free(centroid);
        var pos: usize = header_size;
        for (centroid) |*component| {
            component.* = @bitCast(std.mem.readInt(u32, data[pos..][0..4], .little));
            pos += 4;
        }

        return .{
            .posting_id = std.mem.readInt(u64, data[8..16], .little),
            .generation = std.mem.readInt(u64, data[16..24], .little),
            .mutation_version = std.mem.readInt(u64, data[24..32], .little),
            .payload_version = std.mem.readInt(u64, data[32..40], .little),
            .flags = data[5],
            .parent = std.mem.readInt(u64, data[40..48], .little),
            .level = std.mem.readInt(u16, data[6..8], .little),
            .member_count = std.mem.readInt(u64, data[48..56], .little),
            .bounds_radius = @bitCast(std.mem.readInt(u32, data[56..60], .little)),
            .centroid = centroid,
        };
    }
};

pub const PostingFormat = struct {
    pub const base_magic = [_]u8{ 'A', 'F', 'P', 'B' };
    pub const delta_magic = [_]u8{ 'A', 'F', 'P', 'D' };
    pub const version: u8 = 1;
    pub const delta_compact_version: u8 = 2;

    const base_header_size: usize = 4 + 1 + 8 + 8 + 4;
    const delta_header_size: usize = 4 + 1 + 4;
    const delta_record_size: usize = 8 + 1 + 8;
    const delta_compact_header_size: usize = 4 + 1 + 4 + 8;
    const delta_compact_record_size: usize = 4 + 1 + 8;

    pub fn encodedBaseSize(base: PostingBase) !usize {
        if (base.members.len > std.math.maxInt(u32)) return error.TooLarge;
        return base_header_size + base.members.len * @sizeOf(u64);
    }

    pub fn encodeBase(alloc: std.mem.Allocator, base: PostingBase) ![]u8 {
        const encoded_len = try encodedBaseSize(base);
        var out = try alloc.alloc(u8, encoded_len);
        errdefer alloc.free(out);

        @memcpy(out[0..4], &base_magic);
        out[4] = version;
        std.mem.writeInt(u64, out[5..13], base.posting_id, .little);
        std.mem.writeInt(u64, out[13..21], base.generation, .little);
        std.mem.writeInt(u32, out[21..25], @intCast(base.members.len), .little);

        var pos: usize = base_header_size;
        for (base.members) |member_id| {
            std.mem.writeInt(u64, out[pos..][0..8], member_id, .little);
            pos += 8;
        }
        return out;
    }

    pub fn decodeBase(alloc: std.mem.Allocator, data: []const u8) !OwnedPostingBase {
        if (data.len < base_header_size) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..4], &base_magic)) return error.BadPostingBaseMagic;
        if (data[4] != version) return error.UnsupportedPostingBaseVersion;
        const member_count = std.mem.readInt(u32, data[21..25], .little);
        const expected_len = base_header_size + @as(usize, member_count) * @sizeOf(u64);
        if (data.len != expected_len) return error.Corrupted;

        const members = try alloc.alloc(VectorId, member_count);
        errdefer alloc.free(members);

        var pos: usize = base_header_size;
        for (members) |*member_id| {
            member_id.* = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
        }

        return .{
            .posting_id = std.mem.readInt(u64, data[5..13], .little),
            .generation = std.mem.readInt(u64, data[13..21], .little),
            .members = members,
        };
    }

    pub fn encodeDeltaTail(alloc: std.mem.Allocator, records: []const PostingDeltaRecord) ![]u8 {
        if (records.len > std.math.maxInt(u32)) return error.TooLarge;
        const compact = deltaTailCanUseCompactEncoding(records);
        const encoded_len = if (compact)
            delta_compact_header_size + records.len * delta_compact_record_size
        else
            delta_header_size + records.len * delta_record_size;
        var out = try alloc.alloc(u8, encoded_len);
        errdefer alloc.free(out);

        @memcpy(out[0..4], &delta_magic);
        out[4] = if (compact) delta_compact_version else version;
        std.mem.writeInt(u32, out[5..9], @intCast(records.len), .little);

        if (compact) {
            const base_sequence = records[0].sequence;
            std.mem.writeInt(u64, out[9..17], base_sequence, .little);
            var pos: usize = delta_compact_header_size;
            for (records) |record| {
                std.mem.writeInt(u32, out[pos..][0..4], @intCast(record.sequence - base_sequence), .little);
                pos += 4;
                out[pos] = @intFromEnum(record.op);
                pos += 1;
                std.mem.writeInt(u64, out[pos..][0..8], record.vector_id, .little);
                pos += 8;
            }
        } else {
            var pos: usize = delta_header_size;
            for (records) |record| {
                std.mem.writeInt(u64, out[pos..][0..8], record.sequence, .little);
                pos += 8;
                out[pos] = @intFromEnum(record.op);
                pos += 1;
                std.mem.writeInt(u64, out[pos..][0..8], record.vector_id, .little);
                pos += 8;
            }
        }
        return out;
    }

    pub fn decodeDeltaTail(alloc: std.mem.Allocator, data: []const u8) ![]PostingDeltaRecord {
        if (data.len < delta_header_size) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..4], &delta_magic)) return error.BadPostingDeltaMagic;
        if (data[4] == delta_compact_version) return try decodeCompactDeltaTail(alloc, data);
        if (data[4] != version) return error.UnsupportedPostingDeltaVersion;
        const record_count = std.mem.readInt(u32, data[5..9], .little);
        const expected_len = delta_header_size + @as(usize, record_count) * delta_record_size;
        if (data.len != expected_len) return error.Corrupted;

        const records = try alloc.alloc(PostingDeltaRecord, record_count);
        errdefer alloc.free(records);
        var pos: usize = delta_header_size;
        for (records) |*record| {
            record.sequence = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
            const op_pos = pos;
            record.op = switch (data[op_pos]) {
                @intFromEnum(PostingDeltaOp.insert) => .insert,
                @intFromEnum(PostingDeltaOp.tombstone) => .tombstone,
                @intFromEnum(PostingDeltaOp.replace) => .replace,
                else => return error.UnsupportedPostingDeltaOp,
            };
            pos += 1;
            record.vector_id = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
        }
        return records;
    }

    fn decodeCompactDeltaTail(alloc: std.mem.Allocator, data: []const u8) ![]PostingDeltaRecord {
        if (data.len < delta_compact_header_size) return error.Corrupted;
        const record_count = std.mem.readInt(u32, data[5..9], .little);
        const expected_len = delta_compact_header_size + @as(usize, record_count) * delta_compact_record_size;
        if (data.len != expected_len) return error.Corrupted;
        const base_sequence = std.mem.readInt(u64, data[9..17], .little);

        const records = try alloc.alloc(PostingDeltaRecord, record_count);
        errdefer alloc.free(records);
        var pos: usize = delta_compact_header_size;
        for (records) |*record| {
            const offset = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            const op_pos = pos;
            record.op = switch (data[op_pos]) {
                @intFromEnum(PostingDeltaOp.insert) => .insert,
                @intFromEnum(PostingDeltaOp.tombstone) => .tombstone,
                @intFromEnum(PostingDeltaOp.replace) => .replace,
                else => return error.UnsupportedPostingDeltaOp,
            };
            pos += 1;
            record.vector_id = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
            record.sequence = std.math.add(u64, base_sequence, @as(u64, offset)) catch return error.Corrupted;
        }
        return records;
    }

    fn deltaTailCanUseCompactEncoding(records: []const PostingDeltaRecord) bool {
        if (records.len < 3) return false;
        const base_sequence = records[0].sequence;
        for (records) |record| {
            if (record.sequence < base_sequence) return false;
            if (record.sequence - base_sequence > std.math.maxInt(u32)) return false;
        }
        return true;
    }

    pub fn materializeMembers(
        alloc: std.mem.Allocator,
        base_members: []const VectorId,
        records: []const PostingDeltaRecord,
    ) ![]VectorId {
        var members: std.ArrayList(VectorId) = .empty;
        errdefer members.deinit(alloc);
        try members.ensureTotalCapacity(alloc, base_members.len + records.len);
        try members.appendSlice(alloc, base_members);
        for (records) |record| {
            switch (record.op) {
                .insert, .replace => {
                    removeMemberFromList(&members, record.vector_id);
                    try members.append(alloc, record.vector_id);
                },
                .tombstone => removeMemberFromList(&members, record.vector_id),
            }
        }
        return try members.toOwnedSlice(alloc);
    }

    pub fn materializeMembersAfterGeneration(
        alloc: std.mem.Allocator,
        base_members: []const VectorId,
        records: []const PostingDeltaRecord,
        base_generation: u64,
    ) ![]VectorId {
        var members: std.ArrayList(VectorId) = .empty;
        errdefer members.deinit(alloc);
        try members.ensureTotalCapacity(alloc, base_members.len + records.len);
        try members.appendSlice(alloc, base_members);
        for (records) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            switch (record.op) {
                .insert, .replace => {
                    removeMemberFromList(&members, record.vector_id);
                    try members.append(alloc, record.vector_id);
                },
                .tombstone => removeMemberFromList(&members, record.vector_id),
            }
        }
        return try members.toOwnedSlice(alloc);
    }

    pub fn deltaRecordsAfterGeneration(records: []const PostingDeltaRecord, base_generation: u64) usize {
        var count: usize = 0;
        for (records) |record| {
            if (deltaSequenceGeneration(record.sequence) > base_generation) count += 1;
        }
        return count;
    }

    pub fn deltaSequenceGeneration(sequence: u64) u64 {
        return sequence >> 32;
    }

    fn removeMemberFromList(members: *std.ArrayList(VectorId), vector_id: VectorId) void {
        var i: usize = 0;
        while (i < members.items.len) {
            if (members.items[i] == vector_id) {
                _ = members.orderedRemove(i);
                return;
            }
            i += 1;
        }
    }
};

pub const PostingMaintenanceOptions = struct {
    max_postings: usize = std.math.maxInt(usize),
    refresh_payloads: bool = true,
    refresh_ancestors: bool = true,
    fold_delta_tails: bool = true,
    min_delta_records_to_fold: usize = 64,
    min_tombstone_records_to_fold: usize = 16,
    min_delta_to_base_ratio_bps: u32 = 0,
    max_delta_tail_postings: usize = std.math.maxInt(usize),
    rebalance_layout: bool = false,
    split_full_postings: bool = false,
    max_layout_changes: usize = std.math.maxInt(usize),
    max_boundary_reassignments: usize = 0,
    reassign_dirty_postings: bool = false,
    allow_overfull_reassignment: bool = false,
    max_overfull_reassignment_postings: usize = std.math.maxInt(usize),
    max_over_capacity_reassignment_members: usize = std.math.maxInt(usize),
    boundary_reassignment_min_improvement: f32 = 0.0,
};

pub const PostingMaintenanceResult = struct {
    scanned_nodes: u64 = 0,
    scanned_postings: u64 = 0,
    dirty_postings: u64 = 0,
    repaired_postings: u64 = 0,
    centroid_refreshed: u64 = 0,
    payload_refreshed: u64 = 0,
    ancestor_refresh_roots: u64 = 0,
    split_postings: u64 = 0,
    merged_postings: u64 = 0,
    boundary_reassigned_vectors: u64 = 0,
    boundary_reassignment_capacity_skips: u64 = 0,
    boundary_reassignment_min_source_skips: u64 = 0,
    boundary_reassignment_swap_moves: u64 = 0,
    delta_fold_attempts: u64 = 0,
    delta_fold_skipped: u64 = 0,
    delta_fold_records: u64 = 0,
    skipped_missing: u64 = 0,
    remaining_dirty_postings: u64 = 0,
    remaining_delta_tail_postings: u64 = 0,
    remaining_overfull_postings: u64 = 0,
    remaining_postings_at_capacity: u64 = 0,
    remaining_max_over_capacity_members: u64 = 0,
    limit_reached: bool = false,
};

pub const PostingBacklogStats = struct {
    scanned_nodes: u64 = 0,
    scanned_postings: u64 = 0,
    dirty_postings: u64 = 0,
    centroid_dirty_postings: u64 = 0,
    payload_dirty_postings: u64 = 0,
    min_dirty_mutation_version: u64 = 0,
    max_dirty_version_age: u64 = 0,
    delta_tail_postings: u64 = 0,
    max_delta_tail_records: u64 = 0,
    max_tombstone_tail_records: u64 = 0,
    max_delta_to_base_ratio_bps: u64 = 0,
    overfull_postings: u64 = 0,
    postings_at_capacity: u64 = 0,
    max_over_capacity_members: u64 = 0,
    max_centroid_version_lag: u64 = 0,
    max_payload_version_lag: u64 = 0,
    max_mutation_version: u64 = 0,
    skipped_missing: u64 = 0,

    pub fn needsRepair(self: PostingBacklogStats) bool {
        return self.dirty_postings != 0 or self.delta_tail_postings != 0;
    }

    pub fn write(self: PostingBacklogStats, writer: *std.Io.Writer) !void {
        try writer.print(
            "posting_backlog scanned_nodes={d} scanned_postings={d} dirty_postings={d} centroid_dirty_postings={d} payload_dirty_postings={d} min_dirty_mutation_version={d} max_dirty_version_age={d} delta_tail_postings={d} max_delta_tail_records={d} max_tombstone_tail_records={d} max_delta_to_base_ratio_bps={d} overfull_postings={d} postings_at_capacity={d} max_over_capacity_members={d} max_centroid_version_lag={d} max_payload_version_lag={d} max_mutation_version={d} skipped_missing={d}\n",
            .{
                self.scanned_nodes,
                self.scanned_postings,
                self.dirty_postings,
                self.centroid_dirty_postings,
                self.payload_dirty_postings,
                self.min_dirty_mutation_version,
                self.max_dirty_version_age,
                self.delta_tail_postings,
                self.max_delta_tail_records,
                self.max_tombstone_tail_records,
                self.max_delta_to_base_ratio_bps,
                self.overfull_postings,
                self.postings_at_capacity,
                self.max_over_capacity_members,
                self.max_centroid_version_lag,
                self.max_payload_version_lag,
                self.max_mutation_version,
                self.skipped_missing,
            },
        );
    }
};

pub const PostingStore = struct {
    pub fn view(node: *const types.Node) !PostingView {
        if (!node.is_leaf) return error.ExpectedLeaf;
        return .{
            .id = node.id,
            .parent = node.parent,
            .level = node.level,
            .centroid = node.centroid,
            .members = node.members,
            .state = node.posting_state,
        };
    }

    pub fn copyMemberIds(
        alloc: std.mem.Allocator,
        scratch: anytype,
        posting: PostingView,
    ) ![]VectorId {
        try scratch.ensureMemberIdCapacity(alloc, posting.members.len);
        const member_ids = scratch.member_ids[0..posting.members.len];
        @memcpy(member_ids, posting.members);
        return member_ids;
    }

    pub fn copyQueryMemberIds(
        index: anytype,
        txn: anytype,
        alloc: std.mem.Allocator,
        scratch: anytype,
        posting_view: PostingView,
        profile: anytype,
        now_fn: fn () u64,
        elapsed_fn: fn (u64) u64,
    ) ![]VectorId {
        if (!shouldMaterializeBaseDeltaForQuery(index, @TypeOf(txn))) {
            return try copyMemberIds(alloc, scratch, posting_view);
        }

        const start = now_fn();
        const canonical_base_delta = baseDeltaIsCanonical(index);
        if (canonical_base_delta) {
            if (try copyCachedPostingMembersIfAvailable(alloc, scratch, posting_view, profile)) |cached_member_ids| {
                notePostingOverlay(profile, elapsed_fn(start), cached_member_ids.len, 0, cached_member_ids.len);
                return cached_member_ids;
            }
            notePostingOverlayCacheMiss(profile);
        }

        var base = loadBase(index, txn, posting_view.id, isNotFound) catch |err| {
            if (isNotFound(err)) {
                notePostingOverlayFallback(profile);
                return try copyMemberIds(alloc, scratch, posting_view);
            }
            return err;
        };
        defer base.deinit(index.alloc);
        if (!canonical_base_delta and base.generation < posting_view.state.mutation_version) {
            notePostingOverlayFallback(profile);
            return try copyMemberIds(alloc, scratch, posting_view);
        }

        if (canonical_base_delta and base.generation >= posting_view.state.mutation_version) {
            try scratch.ensureMemberIdCapacity(alloc, base.members.len);
            const member_ids = scratch.member_ids[0..base.members.len];
            @memcpy(member_ids, base.members);
            try cachePostingMembersIfAvailable(scratch, alloc, posting_view, base.members, profile);
            notePostingOverlayDeltaScanSkip(profile);
            notePostingOverlay(profile, elapsed_fn(start), base.members.len, 0, base.members.len);
            return member_ids;
        }

        const deltas = try loadDeltaTail(index, txn, posting_view.id);
        defer index.alloc.free(deltas);
        const materialized = try PostingFormat.materializeMembersAfterGeneration(index.alloc, base.members, deltas, base.generation);
        defer index.alloc.free(materialized);
        if (!canonical_base_delta and !std.mem.eql(VectorId, materialized, posting_view.members)) {
            notePostingOverlayFallback(profile);
            return try copyMemberIds(alloc, scratch, posting_view);
        }

        try scratch.ensureMemberIdCapacity(alloc, materialized.len);
        const member_ids = scratch.member_ids[0..materialized.len];
        @memcpy(member_ids, materialized);
        if (canonical_base_delta) {
            try cachePostingMembersIfAvailable(scratch, alloc, posting_view, materialized, profile);
        }
        notePostingOverlay(profile, elapsed_fn(start), base.members.len, PostingFormat.deltaRecordsAfterGeneration(deltas, base.generation), materialized.len);
        return member_ids;
    }

    pub fn appendMember(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_id: VectorId,
    ) !usize {
        return appendMembers(alloc, node, &.{vector_id});
    }

    pub fn appendMembers(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_ids: []const VectorId,
    ) !usize {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const old_len = node.members.len;
        if (vector_ids.len == 0) return old_len;
        node.members = if (old_len == 0)
            try alloc.alloc(u64, vector_ids.len)
        else
            try alloc.realloc(node.members, old_len + vector_ids.len);
        @memcpy(node.members[old_len..][0..vector_ids.len], vector_ids);
        noteMembersChanged(node);
        return old_len;
    }

    pub fn removeMember(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_id: VectorId,
    ) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const found_index = indexOfMember(node.members, vector_id) orelse return error.NotFound;
        const new_len = node.members.len - 1;
        if (new_len == 0) {
            if (node.members.len > 0) alloc.free(node.members);
            node.members = &.{};
            noteMembersChanged(node);
            return;
        }

        var new_members = try alloc.alloc(u64, new_len);
        errdefer alloc.free(new_members);
        @memcpy(new_members[0..found_index], node.members[0..found_index]);
        @memcpy(new_members[found_index..], node.members[found_index + 1 ..]);
        alloc.free(node.members);
        node.members = new_members;
        noteMembersChanged(node);
    }

    pub fn removeMembers(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_ids: []const VectorId,
    ) !usize {
        if (!node.is_leaf) return error.ExpectedLeaf;
        if (vector_ids.len == 0 or node.members.len == 0) return 0;

        var kept = try alloc.alloc(u64, node.members.len);
        errdefer alloc.free(kept);
        var kept_count: usize = 0;
        var removed_count: usize = 0;
        for (node.members) |member_id| {
            if (containsMember(vector_ids, member_id)) {
                removed_count += 1;
            } else {
                kept[kept_count] = member_id;
                kept_count += 1;
            }
        }

        if (removed_count == 0) {
            alloc.free(kept);
            return 0;
        }

        if (kept_count == 0) {
            alloc.free(kept);
            alloc.free(node.members);
            node.members = &.{};
            noteMembersChanged(node);
            return removed_count;
        }

        const new_members = try alloc.realloc(kept, kept_count);
        alloc.free(node.members);
        node.members = new_members;
        noteMembersChanged(node);
        return removed_count;
    }

    pub fn noteMembersChanged(node: *types.Node) void {
        noteVectorsChanged(node);
    }

    pub fn noteVectorsChanged(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.noteMembersChanged(node.members.len);
    }

    pub fn noteCentroidRefreshed(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.noteCentroidRefreshed();
    }

    pub fn notePayloadRefreshed(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.notePayloadRefreshed();
    }

    pub fn loadState(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingState {
        var key_buf: [12]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting)) catch |err| {
            if (is_not_found(err)) return .{};
            return err;
        };
        return try decodeState(data);
    }

    pub fn saveState(index: anytype, txn: anytype, posting_id: PostingId, state: PostingState) !void {
        var key_buf: [12]u8 = undefined;
        var buf: [state_encoded_size]u8 = undefined;
        try index.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting), encodeState(state, &buf));
    }

    pub fn deleteState(index: anytype, txn: anytype, posting_id: PostingId) !void {
        var key_buf: [12]u8 = undefined;
        try index.deleteNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting));
    }

    pub fn saveBase(index: anytype, txn: anytype, base: PostingBase) !void {
        var key_buf: [10]u8 = undefined;
        const encoded = try PostingFormat.encodeBase(index.alloc, base);
        defer index.alloc.free(encoded);
        const key = hbc.encodePostingBaseKey(&key_buf, base.posting_id);
        try index.putNamespaced(txn, .nodes, key, encoded);
        notePostingBasePut(index, key.len, encoded.len);
    }

    pub fn saveCentroidDirectoryRecord(index: anytype, txn: anytype, record: CentroidDirectoryRecord) !void {
        var key_buf: [10]u8 = undefined;
        const encoded = try CentroidDirectoryFormat.encode(index.alloc, record);
        defer index.alloc.free(encoded);
        const key = hbc.encodeCentroidDirectoryKey(&key_buf, record.posting_id);
        try index.putNamespaced(txn, .nodes, key, encoded);
        noteCentroidDirectoryPut(index, key.len, encoded.len);
    }

    pub fn loadCentroidDirectoryRecord(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedCentroidDirectoryRecord {
        var key_buf: [10]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodeCentroidDirectoryKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        return try CentroidDirectoryFormat.decode(index.alloc, data);
    }

    pub fn loadCentroidDirectoryRecords(index: anytype, txn: anytype) ![]OwnedCentroidDirectoryRecord {
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [2]u8 = undefined;
        const prefix = hbc.encodeCentroidDirectoryPrefix(&prefix_buf);
        var out: std.ArrayList(OwnedCentroidDirectoryRecord) = .empty;
        errdefer {
            for (out.items) |*record| record.deinit(index.alloc);
            out.deinit(index.alloc);
        }

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.centroidDirectoryKeyMatches(entry.key)) break;
            try out.append(index.alloc, try CentroidDirectoryFormat.decode(index.alloc, entry.value));
            maybe_entry = try cursor.next();
        }
        return try out.toOwnedSlice(index.alloc);
    }

    pub fn loadBase(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedPostingBase {
        var key_buf: [10]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodePostingBaseKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        return try PostingFormat.decodeBase(index.alloc, data);
    }

    pub fn deleteBaseAndCentroidRecords(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !void {
        var base_key_buf: [10]u8 = undefined;
        index.deleteNamespaced(txn, .nodes, hbc.encodePostingBaseKey(&base_key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) {} else return err;
        };
        var centroid_key_buf: [10]u8 = undefined;
        index.deleteNamespaced(txn, .nodes, hbc.encodeCentroidDirectoryKey(&centroid_key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) {} else return err;
        };
    }

    pub fn deleteDeltaTailIfSupported(index: anytype, txn: anytype, posting_id: PostingId) !void {
        if (comptime !canScanDeltaTail(@TypeOf(index), @TypeOf(txn))) return;
        _ = try deleteDeltaTail(index, txn, posting_id);
    }

    pub fn deletePostingArtifacts(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !void {
        try deleteBaseAndCentroidRecords(index, txn, posting_id, is_not_found);
        try deleteDeltaTailIfSupported(index, txn, posting_id);
    }

    pub fn appendDelta(index: anytype, txn: anytype, posting_id: PostingId, record: PostingDeltaRecord) !void {
        try appendDeltaRecords(index, txn, posting_id, &.{record});
    }

    pub fn appendDeltaRecords(index: anytype, txn: anytype, posting_id: PostingId, records: []const PostingDeltaRecord) !void {
        if (records.len == 0) return;
        var key_buf: [18]u8 = undefined;
        const encoded = try PostingFormat.encodeDeltaTail(index.alloc, records);
        defer index.alloc.free(encoded);
        const key = hbc.encodePostingDeltaKey(&key_buf, posting_id, records[0].sequence);
        try appendNamespaced(index, txn, .nodes, key, encoded);
        notePostingDeltaAppend(index, key.len, encoded.len, records.len);
    }

    pub fn loadDeltaTail(index: anytype, txn: anytype, posting_id: PostingId) ![]PostingDeltaRecord {
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var out: std.ArrayList(PostingDeltaRecord) = .empty;
        errdefer out.deinit(index.alloc);

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            const decoded = try PostingFormat.decodeDeltaTail(index.alloc, entry.value);
            defer index.alloc.free(decoded);
            try out.appendSlice(index.alloc, decoded);
            maybe_entry = try cursor.next();
        }
        return try out.toOwnedSlice(index.alloc);
    }

    pub fn materializeBaseDeltaMembers(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]VectorId {
        var base = try loadBase(index, txn, posting_id, is_not_found);
        defer base.deinit(index.alloc);
        const deltas = try loadDeltaTail(index, txn, posting_id);
        defer index.alloc.free(deltas);
        return try PostingFormat.materializeMembersAfterGeneration(index.alloc, base.members, deltas, base.generation);
    }

    fn deltaTailShouldFold(base_member_count: usize, deltas: []const PostingDeltaRecord, options: FoldDeltaTailOptions) bool {
        if (deltas.len == 0) return true;
        if (deltas.len >= options.min_delta_records) return true;

        var tombstones: usize = 0;
        for (deltas) |record| {
            if (record.op == .tombstone) tombstones += 1;
        }
        if (options.min_tombstone_records != 0 and tombstones >= options.min_tombstone_records) return true;
        if (options.min_delta_to_base_ratio_bps != 0) {
            const denominator = @max(base_member_count, @as(usize, 1));
            const ratio_bps = (deltas.len * 10_000) / denominator;
            if (ratio_bps >= options.min_delta_to_base_ratio_bps) return true;
        }
        return false;
    }

    pub fn foldDeltaTailIntoBaseWithOptions(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        is_not_found: fn (anyerror) bool,
        options: FoldDeltaTailOptions,
    ) !FoldDeltaTailResult {
        var base = try loadBase(index, txn, posting_id, is_not_found);
        defer base.deinit(index.alloc);
        const deltas = try loadDeltaTail(index, txn, posting_id);
        defer index.alloc.free(deltas);
        if (deltas.len == 0) {
            return .{
                .base_member_count = base.members.len,
                .materialized_member_count = base.members.len,
                .next_generation = base.generation,
            };
        }
        if (!deltaTailShouldFold(base.members.len, deltas, options)) {
            return .{
                .delta_records = deltas.len,
                .base_member_count = base.members.len,
                .materialized_member_count = base.members.len,
                .next_generation = base.generation,
                .skipped = true,
            };
        }

        const materialized = try PostingFormat.materializeMembersAfterGeneration(index.alloc, base.members, deltas, base.generation);
        defer index.alloc.free(materialized);
        const next_generation = base.generation +| 1;
        const folded_base = PostingBase{
            .posting_id = posting_id,
            .generation = next_generation,
            .members = materialized,
        };
        var base_key_buf: [10]u8 = undefined;
        const written_base_key_bytes = hbc.encodePostingBaseKey(&base_key_buf, posting_id).len;
        const written_base_value_bytes = try PostingFormat.encodedBaseSize(folded_base);
        try saveBase(index, txn, folded_base);
        const deleted_tail = try deleteDeltaTail(index, txn, posting_id);
        notePostingDeltaFold(index, PostingFormat.deltaRecordsAfterGeneration(deltas, base.generation), base.members.len, materialized.len, deleted_tail, written_base_key_bytes, written_base_value_bytes);
        return .{
            .delta_records = deltas.len,
            .base_member_count = base.members.len,
            .materialized_member_count = materialized.len,
            .deleted_tail_keys = deleted_tail.keys,
            .deleted_tail_key_bytes = deleted_tail.key_bytes,
            .deleted_tail_value_bytes = deleted_tail.value_bytes,
            .written_base_key_bytes = written_base_key_bytes,
            .written_base_value_bytes = written_base_value_bytes,
            .next_generation = next_generation,
        };
    }

    pub fn foldDeltaTailIntoBase(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !FoldDeltaTailResult {
        return try foldDeltaTailIntoBaseWithOptions(index, txn, posting_id, is_not_found, .{});
    }

    pub fn recomputeCentroid(index: anytype, txn: anytype, node: *types.Node) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        if (node.members.len == 0) {
            @memset(node.centroid, 0);
            noteCentroidRefreshed(node);
            return;
        }

        index.write_profile.centroid_recompute_calls += 1;
        index.write_profile.centroid_recompute_members_total += @intCast(node.members.len);
        if (node.members.len > index.write_profile.centroid_recompute_members_max) {
            index.write_profile.centroid_recompute_members_max = @intCast(node.members.len);
            if (indexHasExternalVectorLoader(index) and node.members.len > index.config.max_cached_vectors) {
                std.log.warn(
                    "hbc centroid recompute external posting_members={} max_cached_vectors={} active_count={} node_count={}",
                    .{ node.members.len, index.config.max_cached_vectors, index.metadata.active_count, index.metadata.node_count },
                );
            }
        }

        if (node.centroid.len != index.config.dims) {
            if (node.centroid.len > 0) index.alloc.free(node.centroid);
            node.centroid = try index.alloc.alloc(f32, index.config.dims);
        }
        @memset(node.centroid, 0);

        const vector_scratch = try index.alloc.alloc(f32, index.config.dims);
        defer index.alloc.free(vector_scratch);
        const transformed = try index.alloc.alloc(f32, index.config.dims);
        defer index.alloc.free(transformed);

        for (node.members) |member_id| {
            const v = try index.getVectorScratch(txn, member_id, vector_scratch);
            _ = index.transformVector(v, transformed);
            vec.add(node.centroid, transformed);
        }
        vec.scale(1.0 / @as(f32, @floatFromInt(node.members.len)), node.centroid);
        normalizeCentroidForMetric(index, node.centroid);
        noteCentroidRefreshed(node);
    }

    pub fn loadTransformedVectorsForQuantizedRefresh(
        index: anytype,
        txn: anytype,
        node: *const types.Node,
        vectors: []f32,
        options: anytype,
    ) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const dims: usize = @intCast(index.metadata.dims);
        if (vectors.len < node.members.len * dims) return error.BufferTooSmall;

        const raw_scratch = try index.alloc.alloc(f32, dims);
        defer index.alloc.free(raw_scratch);
        const transformed_scratch = try index.alloc.alloc(f32, dims);
        defer index.alloc.free(transformed_scratch);

        for (node.members, 0..) |member_id, i| {
            const raw_v = try getBatchVectorScratch(index, txn, member_id, raw_scratch, options);
            const transformed = index.transformVector(raw_v, transformed_scratch);
            @memcpy(std.mem.sliceAsBytes(vectors[i * dims ..][0..dims]), std.mem.sliceAsBytes(transformed));
        }
    }

    pub fn refreshQuantizedPayload(
        index: anytype,
        txn: anytype,
        node: *const types.Node,
        vectors: []const f32,
        now_fn: fn () u64,
        elapsed_fn: fn (u64) u64,
    ) !void {
        const posting = try view(node);
        const count = posting.members.len;
        const dims: usize = @intCast(index.metadata.dims);
        if (vectors.len < count * dims) return error.BufferTooSmall;

        if (index.getCachedQuantizedPtr(posting.id)) |cached| {
            switch (cached.*) {
                .nonquant => |*set| {
                    if (!posting.usesNonQuantizedPayload()) {
                        const compute_start = now_fn();
                        var fresh: hbc_runtime.QuantizedSet = .{ .rabit = try index.quantizer.quantize(posting.centroid, vectors, count) };
                        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                        defer fresh.deinit(index.alloc);
                        const store_start = now_fn();
                        try index.saveQuantized(txn, posting.id, &fresh);
                        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                        return;
                    }
                    set.vectors.dims = @intCast(dims);
                    set.vectors.count = @intCast(count);
                    if (set.vectors.data.len == 0) {
                        set.vectors.data = try index.alloc.alloc(f32, count * dims);
                    } else {
                        set.vectors.data = try index.alloc.realloc(set.vectors.data, count * dims);
                    }
                    @memcpy(set.vectors.data, vectors[0 .. count * dims]);
                    noteMutatedCachedQuantized(index, posting.id);
                    const store_start = now_fn();
                    try index.putQuantizedCached(txn, posting.id, cached);
                    index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                },
                .rabit => |*set| {
                    if (posting.usesNonQuantizedPayload()) {
                        const compute_start = now_fn();
                        var fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
                            .vectors = .{
                                .dims = @intCast(dims),
                                .count = @intCast(count),
                                .data = try index.alloc.dupe(f32, vectors[0 .. count * dims]),
                            },
                        } };
                        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                        defer fresh.deinit(index.alloc);
                        const store_start = now_fn();
                        try index.saveQuantized(txn, posting.id, &fresh);
                        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                        return;
                    }
                    const compute_start = now_fn();
                    try index.quantizer.quantizeInto(set, posting.centroid, vectors, count);
                    index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                    noteMutatedCachedQuantized(index, posting.id);
                    const store_start = now_fn();
                    try index.putQuantizedCached(txn, posting.id, cached);
                    index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                },
            }
        }

        const compute_start = now_fn();
        var qs: hbc_runtime.QuantizedSet = if (posting.usesNonQuantizedPayload())
            .{ .nonquant = .{
                .vectors = .{
                    .dims = @intCast(dims),
                    .count = @intCast(count),
                    .data = try index.alloc.dupe(f32, vectors[0 .. count * dims]),
                },
            } }
        else
            .{ .rabit = try index.quantizer.quantize(posting.centroid, vectors, count) };
        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        defer qs.deinit(index.alloc);
        const store_start = now_fn();
        try index.saveQuantized(txn, posting.id, &qs);
        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
    }
};

const state_format_version: u8 = 1;
const state_flag_dirty: u8 = 1 << 0;
const state_flag_centroid_dirty: u8 = 1 << 1;
const state_flag_payload_dirty: u8 = 1 << 2;
const state_encoded_size: usize = 1 + 1 + 8 + 8 + 8;

fn encodeState(state: PostingState, buf: *[state_encoded_size]u8) []const u8 {
    buf[0] = state_format_version;
    buf[1] = (if (state.dirty) state_flag_dirty else 0) |
        (if (state.centroid_dirty) state_flag_centroid_dirty else 0) |
        (if (state.payload_dirty) state_flag_payload_dirty else 0);
    std.mem.writeInt(u64, buf[2..10], state.mutation_version, .little);
    std.mem.writeInt(u64, buf[10..18], state.centroid_version, .little);
    std.mem.writeInt(u64, buf[18..26], state.payload_version, .little);
    return buf;
}

pub fn decodeState(data: []const u8) !PostingState {
    if (data.len < state_encoded_size) return error.Corrupted;
    if (data[0] != state_format_version) return error.UnsupportedPostingStateVersion;
    const flags = data[1];
    return .{
        .mutation_version = std.mem.readInt(u64, data[2..10], .little),
        .centroid_version = std.mem.readInt(u64, data[10..18], .little),
        .payload_version = std.mem.readInt(u64, data[18..26], .little),
        .dirty = (flags & state_flag_dirty) != 0,
        .centroid_dirty = (flags & state_flag_centroid_dirty) != 0,
        .payload_dirty = (flags & state_flag_payload_dirty) != 0,
    };
}

fn indexOfMember(members: []const VectorId, vector_id: VectorId) ?usize {
    for (members, 0..) |member_id, i| {
        if (member_id == vector_id) return i;
    }
    return null;
}

fn containsMember(members: []const VectorId, vector_id: VectorId) bool {
    return indexOfMember(members, vector_id) != null;
}

fn indexHasExternalVectorLoader(index: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "hasExternalVectorLoader")) return index.hasExternalVectorLoader();
    return false;
}

fn normalizeCentroidForMetric(index: anytype, centroid: []f32) void {
    if (index.config.metric == .cosine and centroid.len > 0) {
        _ = vec.normalize(centroid);
    }
}

fn batchVectorLookup(options: anytype) ?hbc_runtime.BatchVectorLookup {
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "batch_vectors")) return options.batch_vectors;
    return null;
}

fn getBatchVectorScratch(index: anytype, txn: anytype, vector_id: VectorId, scratch: []f32, options: anytype) ![]const f32 {
    if (batchVectorLookup(options)) |lookup| {
        if (lookup.get(vector_id)) |vector| {
            if (vector.len > scratch.len) return error.BufferTooSmall;
            return vector;
        }
    }
    return try index.getVectorScratch(txn, vector_id, scratch);
}

fn noteMutatedCachedQuantized(index: anytype, posting_id: PostingId) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "noteMutatedCachedQuantized")) {
        index.noteMutatedCachedQuantized(posting_id);
    }
}

fn appendNamespaced(index: anytype, txn: anytype, comptime namespace: anytype, key: []const u8, value: []const u8) !void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "appendNamespaced")) {
        return try index.appendNamespaced(txn, namespace, key, value);
    }
    return try index.putNamespaced(txn, namespace, key, value);
}

fn openNamespacedCursor(index: anytype, txn: anytype, comptime namespace: anytype) !vector_store.Cursor {
    const TxnRaw = @TypeOf(txn);
    switch (comptime namespaceCursorDepth(TxnRaw)) {
        1 => return try txn.openCursor(namespace),
        2 => return try txn.*.openCursor(namespace),
        else => {},
    }
    switch (comptime namespaceBatchDepth(TxnRaw)) {
        1 => return try txn.openCursor(namespace),
        2 => return try txn.*.openCursor(namespace),
        else => {},
    }
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "openNamespacedCursor")) {
        return try index.openNamespacedCursor(index.alloc, txn, namespace);
    }
    return try txn.openCursor(namespace);
}

pub fn txnSupportsDeltaTailScan(comptime T: type) bool {
    return namespaceCursorDepth(T) != 0 or namespaceBatchDepth(T) != 0;
}

fn canScanDeltaTail(comptime IndexParam: type, comptime Txn: type) bool {
    if (txnSupportsDeltaTailScan(Txn)) return true;
    const Index = switch (@typeInfo(IndexParam)) {
        .pointer => |ptr| ptr.child,
        else => IndexParam,
    };
    return @hasDecl(Index, "openNamespacedCursor");
}

fn namespaceCursorDepth(comptime T: type) u8 {
    return namespaceTxnDepth(T, true);
}

fn namespaceBatchDepth(comptime T: type) u8 {
    return namespaceTxnDepth(T, false);
}

fn namespaceTxnDepth(comptime T: type, comptime cursor_txn: bool) u8 {
    return switch (@typeInfo(T)) {
        .pointer => |ptr| blk: {
            if (namespaceTxnChildMatches(ptr.child, cursor_txn)) break :blk 1;
            switch (@typeInfo(ptr.child)) {
                .pointer => |inner_ptr| {
                    if (namespaceTxnChildMatches(inner_ptr.child, cursor_txn)) break :blk 2;
                },
                else => {},
            }
            break :blk 0;
        },
        else => 0,
    };
}

fn namespaceTxnChildMatches(comptime T: type, comptime cursor_txn: bool) bool {
    if (cursor_txn) {
        return T == vector_store.NamespaceReadTxn or T == vector_store.NamespaceWriteTxn;
    }
    return T == vector_store.NamespaceBatch;
}

fn deleteDeltaTail(index: anytype, txn: anytype, posting_id: PostingId) !DeleteDeltaTailStats {
    const TailKey = struct {
        key: []u8,
        value_len: usize,
    };

    var cursor = try openNamespacedCursor(index, txn, .nodes);
    defer cursor.close();

    var prefix_buf: [10]u8 = undefined;
    const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
    var keys: std.ArrayList(TailKey) = .empty;
    defer {
        for (keys.items) |entry| index.alloc.free(entry.key);
        keys.deinit(index.alloc);
    }

    var maybe_entry = try cursor.seekAtOrAfter(prefix);
    while (maybe_entry) |entry| {
        if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
        try keys.append(index.alloc, .{
            .key = try index.alloc.dupe(u8, entry.key),
            .value_len = entry.value.len,
        });
        maybe_entry = try cursor.next();
    }

    var stats = DeleteDeltaTailStats{};
    for (keys.items) |entry| {
        try index.deleteNamespaced(txn, .nodes, entry.key);
        stats.keys += 1;
        stats.key_bytes += entry.key.len;
        stats.value_bytes += entry.value_len;
    }
    return stats;
}

fn notePostingBasePut(index: anytype, key_len: usize, value_len: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.posting_base_put_calls += 1;
    index.write_profile.posting_base_key_bytes += @intCast(key_len);
    index.write_profile.posting_base_value_bytes += @intCast(value_len);
}

fn noteCentroidDirectoryPut(index: anytype, key_len: usize, value_len: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.centroid_directory_put_calls += 1;
    index.write_profile.centroid_directory_key_bytes += @intCast(key_len);
    index.write_profile.centroid_directory_value_bytes += @intCast(value_len);
}

fn shouldShadowAssignmentMap(index: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "config")) return false;
    return switch (index.config.posting_storage_mode) {
        .shadow_base_delta => true,
        .base_delta, .packed_hbc => false,
    };
}

fn shouldDeriveAssignmentRecordFromVecLeaf(index: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "config")) return false;
    return index.config.posting_storage_mode == .base_delta;
}

fn nextAssignmentVersion(index: anytype, txn: anytype, vector_id: VectorId) !u64 {
    const current = AssignmentMap.getRecord(index, txn, vector_id, isNotFound) catch |err| {
        if (isNotFound(err)) return 1;
        return err;
    };
    return current.version +| 1;
}

fn noteAssignmentMapPut(index: anytype, key_len: usize, value_len: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.assignment_map_put_calls += 1;
    index.write_profile.assignment_map_key_bytes += @intCast(key_len);
    index.write_profile.assignment_map_value_bytes += @intCast(value_len);
}

fn noteAssignmentMapDelete(index: anytype, key_len: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.assignment_map_delete_calls += 1;
    index.write_profile.assignment_map_key_bytes += @intCast(key_len);
}

fn notePostingDeltaAppend(index: anytype, key_len: usize, value_len: usize, record_count: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.posting_delta_append_calls += 1;
    index.write_profile.posting_delta_records += @intCast(record_count);
    index.write_profile.posting_delta_key_bytes += @intCast(key_len);
    index.write_profile.posting_delta_value_bytes += @intCast(value_len);
}

fn notePostingDeltaFold(index: anytype, record_count: usize, base_member_count: usize, materialized_member_count: usize, deleted_tail: DeleteDeltaTailStats, written_base_key_bytes: usize, written_base_value_bytes: usize) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    index.write_profile.posting_delta_fold_calls += 1;
    index.write_profile.posting_delta_fold_records += @intCast(record_count);
    index.write_profile.posting_delta_fold_base_members += @intCast(base_member_count);
    index.write_profile.posting_delta_fold_materialized_members += @intCast(materialized_member_count);
    index.write_profile.posting_delta_fold_deleted_tail_keys += @intCast(deleted_tail.keys);
    index.write_profile.posting_delta_fold_deleted_tail_key_bytes += @intCast(deleted_tail.key_bytes);
    index.write_profile.posting_delta_fold_deleted_tail_value_bytes += @intCast(deleted_tail.value_bytes);
    index.write_profile.posting_delta_fold_written_base_key_bytes += @intCast(written_base_key_bytes);
    index.write_profile.posting_delta_fold_written_base_value_bytes += @intCast(written_base_value_bytes);
}

fn shouldMaterializeBaseDeltaForQuery(index: anytype, comptime Txn: type) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "config")) return false;
    if (index.config.posting_storage_mode != .shadow_base_delta and
        index.config.posting_storage_mode != .base_delta) return false;
    if (comptime txnSupportsDeltaTailScan(Txn)) return true;
    return comptime @hasDecl(Index, "openNamespacedCursor");
}

fn baseDeltaIsCanonical(index: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "config")) return false;
    return index.config.posting_storage_mode == .base_delta;
}

fn notePostingOverlay(profile: anytype, elapsed_ns: u64, base_member_count: usize, delta_count: usize, materialized_member_count: usize) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_overlay_calls")) return;
    profile.posting_overlay_calls += 1;
    profile.posting_overlay_ns += elapsed_ns;
    profile.posting_overlay_base_members += @intCast(base_member_count);
    profile.posting_overlay_delta_records += @intCast(delta_count);
    profile.posting_overlay_materialized_members += @intCast(materialized_member_count);
}

fn copyCachedPostingMembersIfAvailable(
    alloc: std.mem.Allocator,
    scratch: anytype,
    posting_view: PostingView,
    profile: anytype,
) !?[]VectorId {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (comptime !@hasDecl(Scratch, "cachedPostingMembers")) return null;
    const cached = scratch.cachedPostingMembers(posting_view.id, posting_view.state.mutation_version) orelse return null;
    try scratch.ensureMemberIdCapacity(alloc, cached.len);
    const member_ids = scratch.member_ids[0..cached.len];
    @memcpy(member_ids, cached);
    notePostingOverlayCacheHit(profile);
    return member_ids;
}

fn cachePostingMembersIfAvailable(
    scratch: anytype,
    alloc: std.mem.Allocator,
    posting_view: PostingView,
    members: []const VectorId,
    profile: anytype,
) !void {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (comptime !@hasDecl(Scratch, "cachePostingMembers")) return;
    const result = try scratch.cachePostingMembers(alloc, posting_view.id, posting_view.state.mutation_version, members);
    notePostingOverlayCacheResult(profile, result.evictions, result.admission_skips, result.member_bytes);
}

fn notePostingOverlayCacheHit(profile: anytype) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_overlay_cache_hits")) return;
    profile.posting_overlay_cache_hits += 1;
}

fn notePostingOverlayCacheMiss(profile: anytype) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_overlay_cache_misses")) return;
    profile.posting_overlay_cache_misses += 1;
}

fn notePostingOverlayCacheResult(profile: anytype, evictions: u64, admission_skips: u64, member_bytes: u64) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime @hasField(Profile, "posting_overlay_cache_evictions")) {
        profile.posting_overlay_cache_evictions += evictions;
    }
    if (comptime @hasField(Profile, "posting_overlay_cache_admission_skips")) {
        profile.posting_overlay_cache_admission_skips += admission_skips;
    }
    if (comptime @hasField(Profile, "posting_overlay_cache_member_bytes")) {
        profile.posting_overlay_cache_member_bytes = member_bytes;
    }
}

fn notePostingOverlayDeltaScanSkip(profile: anytype) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_overlay_delta_scan_skips")) return;
    profile.posting_overlay_delta_scan_skips += 1;
}

fn notePostingOverlayFallback(profile: anytype) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_overlay_fallbacks")) return;
    profile.posting_overlay_fallbacks += 1;
}

fn isNotFound(err: anyerror) bool {
    return err == error.NotFound;
}

pub const AssignmentMap = struct {
    pub fn put(index: anytype, txn: anytype, vector_id: VectorId, posting_id: PostingId) !void {
        var key_buf: [10]u8 = undefined;
        var val_buf: [8]u8 = undefined;
        val_buf = @bitCast(std.mem.nativeToLittle(u64, posting_id));
        try index.putNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id), &val_buf);
        if (shouldShadowAssignmentMap(index)) {
            try putRecord(index, txn, .{
                .vector_id = vector_id,
                .posting_id = posting_id,
                .version = (try nextAssignmentVersion(index, txn, vector_id)),
                .vector_ref = vector_id,
                .flags = AssignmentFormat.current_flag,
            });
        }
    }

    pub fn get(index: anytype, txn: anytype, vector_id: VectorId) !PostingId {
        var key_buf: [10]u8 = undefined;
        const data = try index.getNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id));
        if (data.len < @sizeOf(u64)) return error.Corrupted;
        return std.mem.readInt(u64, data[0..8], .little);
    }

    pub fn delete(index: anytype, txn: anytype, vector_id: VectorId) !void {
        var key_buf: [10]u8 = undefined;
        index.deleteNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id)) catch |err| {
            if (!isNotFound(err)) return err;
        };
        if (shouldShadowAssignmentMap(index)) {
            try deleteRecord(index, txn, vector_id);
        }
    }

    pub fn putRecord(index: anytype, txn: anytype, record: AssignmentRecord) !void {
        var key_buf: [10]u8 = undefined;
        var val_buf: [AssignmentFormat.encoded_size]u8 = undefined;
        const key = hbc.encodeAssignmentKey(&key_buf, record.vector_id);
        const value = AssignmentFormat.encode(record, &val_buf);
        try index.putNamespaced(txn, .vecs, key, value);
        noteAssignmentMapPut(index, key.len, value.len);
    }

    pub fn getRecord(index: anytype, txn: anytype, vector_id: VectorId, is_not_found: fn (anyerror) bool) !AssignmentRecord {
        if (shouldDeriveAssignmentRecordFromVecLeaf(index)) {
            const posting_id = get(index, txn, vector_id) catch |err| {
                if (is_not_found(err)) return error.NotFound;
                return err;
            };
            return .{
                .vector_id = vector_id,
                .posting_id = posting_id,
                .version = 1,
                .vector_ref = vector_id,
                .flags = AssignmentFormat.current_flag,
            };
        }
        var key_buf: [10]u8 = undefined;
        const data = index.getNamespaced(txn, .vecs, hbc.encodeAssignmentKey(&key_buf, vector_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        var record = try AssignmentFormat.decode(data);
        if (record.vector_id == 0) record.vector_id = vector_id;
        if (record.vector_ref == 0) record.vector_ref = vector_id;
        return record;
    }

    pub fn deleteRecord(index: anytype, txn: anytype, vector_id: VectorId) !void {
        var key_buf: [10]u8 = undefined;
        const key = hbc.encodeAssignmentKey(&key_buf, vector_id);
        index.deleteNamespaced(txn, .vecs, key) catch |err| {
            if (isNotFound(err)) return;
            return err;
        };
        noteAssignmentMapDelete(index, key.len);
    }
};

pub const CentroidDirectory = struct {
    pub const Probe = struct {
        posting_id: PostingId,
        distance: f32,
        error_bound: f32 = 0,
    };

    pub fn findPosting(
        index: anytype,
        txn: anytype,
        root_id: PostingId,
        query: []const f32,
        allow_quantized: bool,
    ) !PostingId {
        return try index.findLeafWithOptions(txn, root_id, query, allow_quantized);
    }

    // Current HBC remains the first centroid directory implementation. This
    // type is intentionally thin for now; later implementations can expose the
    // same "query to posting IDs" contract without changing PostingStore.
};

test "posting view rejects internal nodes" {
    var children = [_]u64{2};
    const node = types.Node{
        .id = 1,
        .is_leaf = false,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = children[0..],
        .members = &.{},
    };
    try std.testing.expectError(error.ExpectedLeaf, PostingStore.view(&node));
}

test "posting view exposes leaf as posting" {
    var centroid = [_]f32{ 1.0, 2.0 };
    var members = [_]u64{ 10, 20 };
    const node = types.Node{
        .id = 7,
        .is_leaf = true,
        .level = 1,
        .parent = 3,
        .centroid = centroid[0..],
        .children = &.{},
        .members = members[0..],
    };
    const posting = try PostingStore.view(&node);
    try std.testing.expectEqual(@as(PostingId, 7), posting.id);
    try std.testing.expectEqual(@as(PostingId, 3), posting.parent);
    try std.testing.expectEqualSlices(u64, members[0..], posting.members);
    try std.testing.expect(!posting.usesNonQuantizedPayload());
}

test "posting store appends and removes members" {
    const alloc = std.testing.allocator;
    const members = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    var node = types.Node{
        .id = 7,
        .is_leaf = true,
        .level = 1,
        .parent = 3,
        .centroid = &.{},
        .children = &.{},
        .members = members,
    };
    defer node.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), try PostingStore.appendMember(alloc, &node, 3));
    try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 2, 3 }, node.members);

    try PostingStore.removeMember(alloc, &node, 2);
    try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 3 }, node.members);

    const removed = try PostingStore.removeMembers(alloc, &node, &[_]u64{ 1, 9 });
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqualSlices(u64, &[_]u64{3}, node.members);
}

test "posting state tracks dirty versions" {
    var state = PostingState{};
    state.noteMembersChanged(3);
    try std.testing.expectEqual(@as(u64, 1), state.mutation_version);
    try std.testing.expect(state.dirty);
    try std.testing.expect(state.centroid_dirty);
    try std.testing.expect(state.payload_dirty);

    state.noteCentroidRefreshed();
    try std.testing.expectEqual(@as(u64, 1), state.centroid_version);
    try std.testing.expect(!state.centroid_dirty);
    try std.testing.expect(state.dirty);

    state.notePayloadRefreshed();
    try std.testing.expectEqual(@as(u64, 1), state.payload_version);
    try std.testing.expect(!state.payload_dirty);
    try std.testing.expect(!state.dirty);
}

test "posting state encoding round trips" {
    const state = PostingState{
        .mutation_version = 7,
        .centroid_version = 5,
        .payload_version = 6,
        .dirty = true,
        .centroid_dirty = true,
        .payload_dirty = false,
    };
    var buf: [state_encoded_size]u8 = undefined;
    const decoded = try decodeState(encodeState(state, &buf));
    try std.testing.expectEqual(state.mutation_version, decoded.mutation_version);
    try std.testing.expectEqual(state.centroid_version, decoded.centroid_version);
    try std.testing.expectEqual(state.payload_version, decoded.payload_version);
    try std.testing.expectEqual(state.dirty, decoded.dirty);
    try std.testing.expectEqual(state.centroid_dirty, decoded.centroid_dirty);
    try std.testing.expectEqual(state.payload_dirty, decoded.payload_dirty);
}

test "posting base format round trips members" {
    const alloc = std.testing.allocator;
    const members = [_]VectorId{ 10, 20, 30 };

    const encoded = try PostingFormat.encodeBase(alloc, .{
        .posting_id = 7,
        .generation = 11,
        .members = members[0..],
    });
    defer alloc.free(encoded);

    try std.testing.expectEqual(@as(usize, 25 + members.len * @sizeOf(u64)), encoded.len);

    var decoded = try PostingFormat.decodeBase(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(PostingId, 7), decoded.posting_id);
    try std.testing.expectEqual(@as(u64, 11), decoded.generation);
    try std.testing.expectEqualSlices(VectorId, members[0..], decoded.members);
    encoded[4] = 2;
    try std.testing.expectError(error.UnsupportedPostingBaseVersion, PostingFormat.decodeBase(alloc, encoded));
}

test "posting delta tail round trips and overlays base members" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 1, .op = .insert, .vector_id = 40 },
        .{ .sequence = 2, .op = .tombstone, .vector_id = 20 },
        .{ .sequence = 3, .op = .replace, .vector_id = 10 },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);

    try std.testing.expectEqual(PostingFormat.delta_compact_version, encoded[4]);
    try std.testing.expectEqual(@as(usize, 17 + records.len * 13), encoded.len);

    const decoded = try PostingFormat.decodeDeltaTail(alloc, encoded);
    defer alloc.free(decoded);

    try std.testing.expectEqual(records.len, decoded.len);
    for (records, decoded) |expected, actual| {
        try std.testing.expectEqual(expected.sequence, actual.sequence);
        try std.testing.expectEqual(expected.op, actual.op);
        try std.testing.expectEqual(expected.vector_id, actual.vector_id);
    }

    const base_members = [_]VectorId{ 10, 20, 30 };
    const materialized = try PostingFormat.materializeMembers(alloc, base_members[0..], decoded);
    defer alloc.free(materialized);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 30, 40, 10 }, materialized);
    encoded[4] = 99;
    try std.testing.expectError(error.UnsupportedPostingDeltaVersion, PostingFormat.decodeDeltaTail(alloc, encoded));
}

test "posting delta tail keeps single-record values in legacy format" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 7, .op = .insert, .vector_id = 10 },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);

    try std.testing.expectEqual(PostingFormat.version, encoded[4]);
    try std.testing.expectEqual(@as(usize, 9 + records.len * 17), encoded.len);

    const decoded = try PostingFormat.decodeDeltaTail(alloc, encoded);
    defer alloc.free(decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(records[0].sequence, decoded[0].sequence);
    try std.testing.expectEqual(records[0].op, decoded[0].op);
    try std.testing.expectEqual(records[0].vector_id, decoded[0].vector_id);
}

test "posting delta tail rejects compact sequence overflow" {
    const alloc = std.testing.allocator;
    var encoded = try alloc.alloc(u8, 17 + 13);
    defer alloc.free(encoded);
    @memcpy(encoded[0..4], &PostingFormat.delta_magic);
    encoded[4] = PostingFormat.delta_compact_version;
    std.mem.writeInt(u32, encoded[5..9], 1, .little);
    std.mem.writeInt(u64, encoded[9..17], std.math.maxInt(u64), .little);
    std.mem.writeInt(u32, encoded[17..21], 1, .little);
    encoded[21] = @intFromEnum(PostingDeltaOp.insert);
    std.mem.writeInt(u64, encoded[22..30], 10, .little);

    try std.testing.expectError(error.Corrupted, PostingFormat.decodeDeltaTail(alloc, encoded));
}

test "posting delta tail rejects unsupported op" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 1, .op = .insert, .vector_id = 10 },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);

    encoded[PostingFormat.delta_header_size + 8] = 99;
    try std.testing.expectError(error.UnsupportedPostingDeltaOp, PostingFormat.decodeDeltaTail(alloc, encoded));
}

test "centroid directory format round trips centroid and stats" {
    const alloc = std.testing.allocator;
    const centroid = [_]f32{ 0.25, -1.5, 3.0 };

    const encoded = try CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 7,
        .generation = 11,
        .mutation_version = 13,
        .payload_version = 17,
        .flags = CentroidDirectoryFormat.dirty_flag | CentroidDirectoryFormat.payload_dirty_flag,
        .parent = 3,
        .level = 1,
        .member_count = 42,
        .bounds_radius = 0.75,
        .centroid = centroid[0..],
    });
    defer alloc.free(encoded);

    var decoded = try CentroidDirectoryFormat.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(PostingId, 7), decoded.posting_id);
    try std.testing.expectEqual(@as(u64, 11), decoded.generation);
    try std.testing.expectEqual(@as(u64, 13), decoded.mutation_version);
    try std.testing.expectEqual(@as(u64, 17), decoded.payload_version);
    try std.testing.expectEqual(CentroidDirectoryFormat.dirty_flag | CentroidDirectoryFormat.payload_dirty_flag, decoded.flags);
    try std.testing.expectEqual(@as(PostingId, 3), decoded.parent);
    try std.testing.expectEqual(@as(u16, 1), decoded.level);
    try std.testing.expectEqual(@as(u64, 42), decoded.member_count);
    try std.testing.expectEqual(@as(f32, 0.75), decoded.bounds_radius);
    try std.testing.expectEqualSlices(f32, centroid[0..], decoded.centroid);
}

test "centroid directory format rejects unsupported current-version flags" {
    const alloc = std.testing.allocator;
    const centroid = [_]f32{0.5};

    const encoded = try CentroidDirectoryFormat.encode(alloc, .{
        .posting_id = 1,
        .generation = 2,
        .parent = 0,
        .level = 0,
        .member_count = 1,
        .flags = CentroidDirectoryFormat.dirty_flag,
        .centroid = centroid[0..],
    });
    defer alloc.free(encoded);

    encoded[5] = 0x80;
    try std.testing.expectError(error.UnsupportedCentroidDirectoryFlags, CentroidDirectoryFormat.decode(alloc, encoded));
}

test "assignment format round trips versioned vector reference" {
    var buf: [AssignmentFormat.encoded_size]u8 = undefined;
    const decoded = try AssignmentFormat.decode(AssignmentFormat.encode(.{
        .vector_id = 10,
        .posting_id = 7,
        .version = 3,
        .vector_ref = 99,
        .flags = AssignmentFormat.current_flag,
    }, &buf));

    try std.testing.expectEqual(@as(VectorId, 0), decoded.vector_id);
    try std.testing.expectEqual(@as(PostingId, 7), decoded.posting_id);
    try std.testing.expectEqual(@as(u64, 3), decoded.version);
    try std.testing.expectEqual(@as(u64, 0), decoded.vector_ref);
    try std.testing.expectEqual(AssignmentFormat.current_flag, decoded.flags);
}

test "assignment format rejects unsupported current-version flags" {
    var buf: [AssignmentFormat.encoded_size]u8 = undefined;
    const encoded = AssignmentFormat.encode(.{
        .vector_id = 10,
        .posting_id = 7,
        .version = 3,
        .vector_ref = 99,
        .flags = AssignmentFormat.current_flag,
    }, &buf);

    encoded[3] = 0;
    try std.testing.expectError(error.UnsupportedAssignmentFlags, AssignmentFormat.decode(encoded));
}

const PostingPersistenceTestIndex = struct {
    const DeltaEntry = struct {
        key: []u8,
        value: []u8,
    };

    alloc: std.mem.Allocator,
    config: types.HBCConfig = .{ .dims = 2 },
    base_key: [10]u8 = undefined,
    base_value: []u8 = &.{},
    centroid_directory_key: [10]u8 = undefined,
    centroid_directory_value: []u8 = &.{},
    legacy_assignment_entries: std.ArrayList(DeltaEntry) = .empty,
    assignment_entries: std.ArrayList(DeltaEntry) = .empty,
    delta_entries: std.ArrayList(DeltaEntry) = .empty,
    write_profile: hbc_runtime.WriteProfile = .{},
    saw_append: bool = false,
    cursor_open_count: u64 = 0,

    fn deinit(self: *PostingPersistenceTestIndex) void {
        if (self.base_value.len > 0) self.alloc.free(self.base_value);
        if (self.centroid_directory_value.len > 0) self.alloc.free(self.centroid_directory_value);
        self.freeEntries(&self.legacy_assignment_entries);
        self.freeEntries(&self.assignment_entries);
        self.freeEntries(&self.delta_entries);
    }

    fn freeEntries(self: *PostingPersistenceTestIndex, entries: *std.ArrayList(DeltaEntry)) void {
        for (entries.items) |entry| {
            self.alloc.free(entry.key);
            self.alloc.free(entry.value);
        }
        entries.deinit(self.alloc);
    }

    fn putEntry(self: *PostingPersistenceTestIndex, entries: *std.ArrayList(DeltaEntry), key: []const u8, value: []const u8) !void {
        for (entries.items) |*entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                self.alloc.free(entry.value);
                entry.value = try self.alloc.dupe(u8, value);
                return;
            }
        }
        try entries.append(self.alloc, .{
            .key = try self.alloc.dupe(u8, key),
            .value = try self.alloc.dupe(u8, value),
        });
    }

    fn getEntry(entries: *const std.ArrayList(DeltaEntry), key: []const u8) ?[]const u8 {
        for (entries.items) |entry| {
            if (std.mem.eql(u8, entry.key, key)) return entry.value;
        }
        return null;
    }

    fn deleteEntry(self: *PostingPersistenceTestIndex, entries: *std.ArrayList(DeltaEntry), key: []const u8) bool {
        var i: usize = 0;
        while (i < entries.items.len) {
            if (std.mem.eql(u8, entries.items[i].key, key)) {
                const removed = entries.orderedRemove(i);
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
                return true;
            }
            i += 1;
        }
        return false;
    }

    pub fn putNamespaced(self: *PostingPersistenceTestIndex, txn: anytype, comptime namespace: anytype, key: []const u8, value: []const u8) !void {
        _ = txn;
        if (key.len != self.base_key.len) return error.UnexpectedKey;
        if (namespace == .nodes and key[0] == 'P' and key[1] == 'B') {
            @memcpy(self.base_key[0..], key);
            if (self.base_value.len > 0) self.alloc.free(self.base_value);
            self.base_value = try self.alloc.dupe(u8, value);
        } else if (namespace == .nodes and key[0] == 'C' and key[1] == 'D') {
            @memcpy(self.centroid_directory_key[0..], key);
            if (self.centroid_directory_value.len > 0) self.alloc.free(self.centroid_directory_value);
            self.centroid_directory_value = try self.alloc.dupe(u8, value);
        } else if (namespace == .vecs and key[0] == 'l' and key[1] == ':') {
            try self.putEntry(&self.legacy_assignment_entries, key, value);
        } else if (namespace == .vecs and key[0] == 'A' and key[1] == 'M') {
            try self.putEntry(&self.assignment_entries, key, value);
        } else {
            return error.UnexpectedKey;
        }
    }

    pub fn appendNamespaced(self: *PostingPersistenceTestIndex, txn: anytype, comptime namespace: anytype, key: []const u8, value: []const u8) !void {
        _ = txn;
        if (namespace != .nodes) return error.UnexpectedNamespace;
        self.saw_append = true;
        try self.delta_entries.append(self.alloc, .{
            .key = try self.alloc.dupe(u8, key),
            .value = try self.alloc.dupe(u8, value),
        });
        std.mem.sort(DeltaEntry, self.delta_entries.items, {}, lessDeltaEntry);
    }

    pub fn getNamespaced(self: *PostingPersistenceTestIndex, txn: anytype, comptime namespace: anytype, key: []const u8) ![]const u8 {
        _ = txn;
        if (namespace == .nodes and self.base_value.len > 0 and std.mem.eql(u8, key, self.base_key[0..])) return self.base_value;
        if (namespace == .nodes and self.centroid_directory_value.len > 0 and std.mem.eql(u8, key, self.centroid_directory_key[0..])) return self.centroid_directory_value;
        if (namespace == .vecs) {
            if (PostingPersistenceTestIndex.getEntry(&self.legacy_assignment_entries, key)) |value| return value;
            if (PostingPersistenceTestIndex.getEntry(&self.assignment_entries, key)) |value| return value;
        }
        return error.NotFound;
    }

    pub fn deleteNamespaced(self: *PostingPersistenceTestIndex, txn: anytype, comptime namespace: anytype, key: []const u8) !void {
        _ = txn;
        if (namespace == .vecs) {
            if (self.deleteEntry(&self.legacy_assignment_entries, key)) return;
            if (self.deleteEntry(&self.assignment_entries, key)) return;
            return error.NotFound;
        }
        if (namespace != .nodes) return error.UnexpectedNamespace;
        if (self.base_value.len > 0 and std.mem.eql(u8, key, self.base_key[0..])) {
            self.alloc.free(self.base_value);
            self.base_value = &.{};
            return;
        }
        if (self.centroid_directory_value.len > 0 and std.mem.eql(u8, key, self.centroid_directory_key[0..])) {
            self.alloc.free(self.centroid_directory_value);
            self.centroid_directory_value = &.{};
            return;
        }
        var i: usize = 0;
        while (i < self.delta_entries.items.len) {
            if (std.mem.eql(u8, self.delta_entries.items[i].key, key)) {
                const removed = self.delta_entries.orderedRemove(i);
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
                return;
            }
            i += 1;
        }
        return error.NotFound;
    }

    pub fn openNamespacedCursor(self: *PostingPersistenceTestIndex, alloc: std.mem.Allocator, txn: anytype, comptime namespace: anytype) !vector_store.Cursor {
        _ = txn;
        if (namespace != .nodes) return error.UnexpectedNamespace;
        self.cursor_open_count += 1;
        return try vector_store.cursorFrom(alloc, PostingPersistenceTestCursor{ .index = self });
    }

    fn lessDeltaEntry(_: void, a: DeltaEntry, b: DeltaEntry) bool {
        return std.mem.order(u8, a.key, b.key) == .lt;
    }
};

fn clonePostingPersistenceState(alloc: std.mem.Allocator, src: *const PostingPersistenceTestIndex) !PostingPersistenceTestIndex {
    var dst = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = src.config,
        .base_key = src.base_key,
        .centroid_directory_key = src.centroid_directory_key,
    };
    errdefer dst.deinit();
    if (src.base_value.len > 0) dst.base_value = try alloc.dupe(u8, src.base_value);
    if (src.centroid_directory_value.len > 0) dst.centroid_directory_value = try alloc.dupe(u8, src.centroid_directory_value);
    for (src.legacy_assignment_entries.items) |entry| {
        try dst.legacy_assignment_entries.append(alloc, .{
            .key = try alloc.dupe(u8, entry.key),
            .value = try alloc.dupe(u8, entry.value),
        });
    }
    for (src.assignment_entries.items) |entry| {
        try dst.assignment_entries.append(alloc, .{
            .key = try alloc.dupe(u8, entry.key),
            .value = try alloc.dupe(u8, entry.value),
        });
    }
    for (src.delta_entries.items) |entry| {
        try dst.delta_entries.append(alloc, .{
            .key = try alloc.dupe(u8, entry.key),
            .value = try alloc.dupe(u8, entry.value),
        });
    }
    return dst;
}

fn expectPostingFamiliesAgree(
    alloc: std.mem.Allocator,
    index: *PostingPersistenceTestIndex,
    txn: anytype,
    posting_id: PostingId,
    expected_generation: u64,
    expected_members: []const VectorId,
) !void {
    var base = try PostingStore.loadBase(index, txn, posting_id, isNotFoundForPostingPersistenceTest);
    defer base.deinit(alloc);
    try std.testing.expectEqual(expected_generation, base.generation);

    const materialized = try PostingStore.materializeBaseDeltaMembers(index, txn, posting_id, isNotFoundForPostingPersistenceTest);
    defer alloc.free(materialized);
    try std.testing.expectEqualSlices(VectorId, expected_members, materialized);

    var centroid = try PostingStore.loadCentroidDirectoryRecord(index, txn, posting_id, isNotFoundForPostingPersistenceTest);
    defer centroid.deinit(alloc);
    try std.testing.expectEqual(posting_id, centroid.posting_id);
    try std.testing.expectEqual(expected_generation, centroid.generation);
    try std.testing.expectEqual(@as(u64, @intCast(expected_members.len)), centroid.member_count);

    for (expected_members) |vector_id| {
        try std.testing.expectEqual(posting_id, try AssignmentMap.get(index, txn, vector_id));
        const assignment = try AssignmentMap.getRecord(index, txn, vector_id, isNotFoundForPostingPersistenceTest);
        try std.testing.expectEqual(vector_id, assignment.vector_id);
        try std.testing.expectEqual(posting_id, assignment.posting_id);
    }
}

const PostingPersistenceTestCursor = struct {
    index: *PostingPersistenceTestIndex,
    pos: ?usize = null,

    pub fn close(_: *@This()) void {}

    pub fn first(self: *@This()) !?vector_store.Entry {
        if (self.index.delta_entries.items.len == 0) return null;
        self.pos = 0;
        return self.entryAt(0);
    }

    pub fn last(self: *@This()) !?vector_store.Entry {
        if (self.index.delta_entries.items.len == 0) return null;
        self.pos = self.index.delta_entries.items.len - 1;
        return self.entryAt(self.pos.?);
    }

    pub fn next(self: *@This()) !?vector_store.Entry {
        const next_pos = if (self.pos) |pos| pos + 1 else 0;
        if (next_pos >= self.index.delta_entries.items.len) return null;
        self.pos = next_pos;
        return self.entryAt(next_pos);
    }

    pub fn prev(self: *@This()) !?vector_store.Entry {
        const pos = self.pos orelse return null;
        if (pos == 0) return null;
        self.pos = pos - 1;
        return self.entryAt(self.pos.?);
    }

    pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?vector_store.Entry {
        for (self.index.delta_entries.items, 0..) |entry, i| {
            if (std.mem.order(u8, entry.key, key) != .lt) {
                self.pos = i;
                return self.entryAt(i);
            }
        }
        return null;
    }

    pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?vector_store.Entry {
        var found: ?usize = null;
        for (self.index.delta_entries.items, 0..) |entry, i| {
            if (std.mem.order(u8, entry.key, key) == .gt) break;
            found = i;
        }
        const pos = found orelse return null;
        self.pos = pos;
        return self.entryAt(pos);
    }

    fn entryAt(self: *@This(), pos: usize) vector_store.Entry {
        const entry = self.index.delta_entries.items[pos];
        return .{ .key = entry.key, .value = entry.value };
    }
};

fn isNotFoundForPostingPersistenceTest(err: anyerror) bool {
    return err == error.NotFound;
}

const PostingQueryMaterializeTestScratch = struct {
    member_ids: []u64 = &.{},

    fn deinit(self: *PostingQueryMaterializeTestScratch, alloc: std.mem.Allocator) void {
        alloc.free(self.member_ids);
        self.* = .{};
    }

    pub fn ensureMemberIdCapacity(self: *PostingQueryMaterializeTestScratch, alloc: std.mem.Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
    }
};

const PostingQueryMaterializeTestProfile = struct {
    posting_overlay_ns: u64 = 0,
    posting_overlay_calls: u64 = 0,
    posting_overlay_base_members: u64 = 0,
    posting_overlay_delta_records: u64 = 0,
    posting_overlay_delta_scan_skips: u64 = 0,
    posting_overlay_materialized_members: u64 = 0,
    posting_overlay_fallbacks: u64 = 0,
    posting_overlay_cache_hits: u64 = 0,
    posting_overlay_cache_misses: u64 = 0,
    posting_overlay_cache_evictions: u64 = 0,
    posting_overlay_cache_admission_skips: u64 = 0,
    posting_overlay_cache_member_bytes: u64 = 0,
};

fn postingQueryTestNow() u64 {
    return 10;
}

fn postingQueryTestElapsed(start: u64) u64 {
    return 25 - start;
}

test "posting store persists base records and appends deltas through namespace helpers" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    const centroid = [_]f32{ 1.0, 2.0 };
    const members = [_]VectorId{ 100, 200 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });

    var expected_base_key: [10]u8 = undefined;
    try std.testing.expectEqualSlices(u8, hbc.encodePostingBaseKey(&expected_base_key, 9), index.base_key[0..]);

    var loaded = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer loaded.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 9), loaded.posting_id);
    try std.testing.expectEqualSlices(VectorId, members[0..], loaded.members);

    try PostingStore.saveCentroidDirectoryRecord(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .parent = 3,
        .level = 1,
        .member_count = members.len,
        .bounds_radius = 0,
        .centroid = centroid[0..],
    });
    var expected_centroid_key: [10]u8 = undefined;
    try std.testing.expectEqualSlices(u8, hbc.encodeCentroidDirectoryKey(&expected_centroid_key, 9), index.centroid_directory_key[0..]);

    var loaded_centroid = try PostingStore.loadCentroidDirectoryRecord(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer loaded_centroid.deinit(alloc);
    try std.testing.expectEqual(@as(PostingId, 9), loaded_centroid.posting_id);
    try std.testing.expectEqual(@as(PostingId, 3), loaded_centroid.parent);
    try std.testing.expectEqual(@as(u64, members.len), loaded_centroid.member_count);
    try std.testing.expectEqualSlices(f32, centroid[0..], loaded_centroid.centroid);

    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = 12,
        .op = .tombstone,
        .vector_id = 100,
    });
    try std.testing.expect(index.saw_append);

    var expected_delta_key: [18]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), index.delta_entries.items.len);
    try std.testing.expectEqualSlices(u8, hbc.encodePostingDeltaKey(&expected_delta_key, 9, 12), index.delta_entries.items[0].key);
    const decoded = try PostingFormat.decodeDeltaTail(alloc, index.delta_entries.items[0].value);
    defer alloc.free(decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(PostingDeltaOp.tombstone, decoded[0].op);
    try std.testing.expectEqual(@as(VectorId, 100), decoded[0].vector_id);
}

test "posting store delete artifacts removes base centroid and delta tail families" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};

    const centroid = [_]f32{ 1.0, 2.0 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = &[_]VectorId{ 100, 200 },
    });
    try PostingStore.saveCentroidDirectoryRecord(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .parent = 3,
        .level = 1,
        .member_count = 2,
        .bounds_radius = 0,
        .centroid = centroid[0..],
    });
    try PostingStore.appendDeltaRecords(&index, &txn, 9, &.{
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .tombstone, .vector_id = 100 },
        .{ .sequence = (@as(u64, 5) << 32) | 2, .op = .insert, .vector_id = 300 },
    });
    try PostingStore.appendDelta(&index, &txn, 10, .{
        .sequence = (@as(u64, 1) << 32) | 1,
        .op = .insert,
        .vector_id = 999,
    });

    try PostingStore.deletePostingArtifacts(&index, &txn, 9, isNotFoundForPostingPersistenceTest);

    try std.testing.expectError(error.NotFound, PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest));
    try std.testing.expectError(error.NotFound, PostingStore.loadCentroidDirectoryRecord(&index, &txn, 9, isNotFoundForPostingPersistenceTest));
    const deleted_tail = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deleted_tail);
    try std.testing.expectEqual(@as(usize, 0), deleted_tail.len);
    const unrelated_tail = try PostingStore.loadDeltaTail(&index, &txn, 10);
    defer alloc.free(unrelated_tail);
    try std.testing.expectEqual(@as(usize, 1), unrelated_tail.len);
}

test "posting store can append multiple delta records in one key" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = &[_]VectorId{ 100, 200 },
    });

    const records = [_]PostingDeltaRecord{
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .insert, .vector_id = 300 },
        .{ .sequence = (@as(u64, 5) << 32) | 2, .op = .tombstone, .vector_id = 100 },
        .{ .sequence = (@as(u64, 5) << 32) | 3, .op = .insert, .vector_id = 400 },
    };
    try PostingStore.appendDeltaRecords(&index, &txn, 9, records[0..]);

    var expected_delta_key: [18]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), index.delta_entries.items.len);
    try std.testing.expectEqualSlices(u8, hbc.encodePostingDeltaKey(&expected_delta_key, 9, records[0].sequence), index.delta_entries.items[0].key);

    const deltas = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas);
    try std.testing.expectEqual(@as(usize, records.len), deltas.len);
    for (records, 0..) |record, i| {
        try std.testing.expectEqual(record.sequence, deltas[i].sequence);
        try std.testing.expectEqual(record.op, deltas[i].op);
        try std.testing.expectEqual(record.vector_id, deltas[i].vector_id);
    }

    const materialized = try PostingStore.materializeBaseDeltaMembers(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer alloc.free(materialized);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 200, 300, 400 }, materialized);

    const folded = try PostingStore.foldDeltaTailIntoBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(usize, records.len), folded.delta_records);
    try std.testing.expectEqual(@as(usize, 1), folded.deleted_tail_keys);
    try std.testing.expectEqual(@as(usize, 18), folded.deleted_tail_key_bytes);
    try std.testing.expect(folded.deleted_tail_value_bytes > 0);
    try std.testing.expectEqual(@as(usize, 10), folded.written_base_key_bytes);
    try std.testing.expectEqual(@as(usize, 49), folded.written_base_value_bytes);
    try std.testing.expectEqual(@as(usize, 0), index.delta_entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_delta_fold_calls);
    try std.testing.expectEqual(@as(u64, records.len), index.write_profile.posting_delta_fold_records);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_delta_fold_deleted_tail_keys);
    try std.testing.expectEqual(@as(u64, 18), index.write_profile.posting_delta_fold_deleted_tail_key_bytes);
    try std.testing.expect(index.write_profile.posting_delta_fold_deleted_tail_value_bytes > 0);
    try std.testing.expectEqual(@as(u64, 10), index.write_profile.posting_delta_fold_written_base_key_bytes);
    try std.testing.expectEqual(@as(u64, 49), index.write_profile.posting_delta_fold_written_base_value_bytes);
}

test "assignment map shadow record increments version while preserving legacy mapping" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .shadow_base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};

    try AssignmentMap.put(&index, &txn, 42, 9);
    try std.testing.expectEqual(@as(PostingId, 9), try AssignmentMap.get(&index, &txn, 42));
    const first = try AssignmentMap.getRecord(&index, &txn, 42, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(VectorId, 42), first.vector_id);
    try std.testing.expectEqual(@as(PostingId, 9), first.posting_id);
    try std.testing.expectEqual(@as(u64, 1), first.version);
    try std.testing.expectEqual(@as(u64, 42), first.vector_ref);

    try AssignmentMap.put(&index, &txn, 42, 11);
    try std.testing.expectEqual(@as(PostingId, 11), try AssignmentMap.get(&index, &txn, 42));
    const second = try AssignmentMap.getRecord(&index, &txn, 42, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(PostingId, 11), second.posting_id);
    try std.testing.expectEqual(@as(u64, 2), second.version);

    try AssignmentMap.delete(&index, &txn, 42);
    try std.testing.expectError(error.NotFound, AssignmentMap.getRecord(&index, &txn, 42, isNotFoundForPostingPersistenceTest));
}

test "base delta assignment record derives from legacy vec leaf mapping" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};

    try AssignmentMap.put(&index, &txn, 42, 9);
    try std.testing.expectEqual(@as(usize, 1), index.legacy_assignment_entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), index.assignment_entries.items.len);

    const assignment = try AssignmentMap.getRecord(&index, &txn, 42, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(VectorId, 42), assignment.vector_id);
    try std.testing.expectEqual(@as(PostingId, 9), assignment.posting_id);
    try std.testing.expectEqual(@as(u64, 1), assignment.version);
    try std.testing.expectEqual(@as(u64, 42), assignment.vector_ref);
    try std.testing.expectEqual(AssignmentFormat.current_flag, assignment.flags);

    try AssignmentMap.delete(&index, &txn, 42);
    try std.testing.expectError(error.NotFound, AssignmentMap.getRecord(&index, &txn, 42, isNotFoundForPostingPersistenceTest));
}

test "posting base delta centroid and assignment families agree after recovered fold" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};

    const centroid_v4 = [_]f32{ 1.0, 2.0 };
    const centroid_v5 = [_]f32{ 1.5, 2.5 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = &[_]VectorId{ 100, 200 },
    });
    try PostingStore.saveCentroidDirectoryRecord(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .mutation_version = 4,
        .payload_version = 4,
        .parent = 3,
        .level = 1,
        .member_count = 2,
        .centroid = centroid_v4[0..],
    });
    try AssignmentMap.put(&index, &txn, 100, 9);
    try AssignmentMap.put(&index, &txn, 200, 9);

    try PostingStore.appendDeltaRecords(&index, &txn, 9, &.{
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .tombstone, .vector_id = 100 },
        .{ .sequence = (@as(u64, 5) << 32) | 2, .op = .insert, .vector_id = 300 },
    });
    try AssignmentMap.delete(&index, &txn, 100);
    try AssignmentMap.put(&index, &txn, 300, 9);

    const folded = try PostingStore.foldDeltaTailIntoBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(u64, 5), folded.next_generation);
    try PostingStore.saveCentroidDirectoryRecord(&index, &txn, .{
        .posting_id = 9,
        .generation = folded.next_generation,
        .mutation_version = 5,
        .payload_version = 5,
        .parent = 3,
        .level = 1,
        .member_count = 2,
        .centroid = centroid_v5[0..],
    });

    var recovered = try clonePostingPersistenceState(alloc, &index);
    defer recovered.deinit();
    var recovered_txn = struct {}{};
    try expectPostingFamiliesAgree(alloc, &recovered, &recovered_txn, 9, 5, &[_]VectorId{ 200, 300 });
    try std.testing.expectError(error.NotFound, AssignmentMap.get(&recovered, &recovered_txn, 100));
    const recovered_deltas = try PostingStore.loadDeltaTail(&recovered, &recovered_txn, 9);
    defer alloc.free(recovered_deltas);
    try std.testing.expectEqual(@as(usize, 0), recovered_deltas.len);
}

test "posting store scans materializes and folds delta tail into a new base" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    const members = [_]VectorId{ 100, 200 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 2,
        .op = .insert,
        .vector_id = 300,
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 1,
        .op = .tombstone,
        .vector_id = 100,
    });
    try PostingStore.appendDelta(&index, &txn, 10, .{
        .sequence = (@as(u64, 1) << 32) | 1,
        .op = .insert,
        .vector_id = 999,
    });

    const deltas = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas);
    try std.testing.expectEqual(@as(usize, 2), deltas.len);
    try std.testing.expectEqual((@as(u64, 5) << 32) | 1, deltas[0].sequence);
    try std.testing.expectEqual((@as(u64, 5) << 32) | 2, deltas[1].sequence);

    const materialized_before = try PostingStore.materializeBaseDeltaMembers(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer alloc.free(materialized_before);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 200, 300 }, materialized_before);

    const folded = try PostingStore.foldDeltaTailIntoBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(usize, 2), folded.delta_records);
    try std.testing.expectEqual(@as(usize, 2), folded.base_member_count);
    try std.testing.expectEqual(@as(usize, 2), folded.materialized_member_count);
    try std.testing.expectEqual(@as(usize, 2), folded.deleted_tail_keys);
    try std.testing.expectEqual(@as(usize, 36), folded.deleted_tail_key_bytes);
    try std.testing.expect(folded.deleted_tail_value_bytes > 0);
    try std.testing.expectEqual(@as(usize, 10), folded.written_base_key_bytes);
    try std.testing.expectEqual(@as(usize, 41), folded.written_base_value_bytes);
    try std.testing.expectEqual(@as(u64, 5), folded.next_generation);

    var loaded = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer loaded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), loaded.generation);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 200, 300 }, loaded.members);

    const deltas_after = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after);
    try std.testing.expectEqual(@as(usize, 0), deltas_after.len);

    const other_posting_deltas = try PostingStore.loadDeltaTail(&index, &txn, 10);
    defer alloc.free(other_posting_deltas);
    try std.testing.expectEqual(@as(usize, 1), other_posting_deltas.len);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_delta_fold_calls);
    try std.testing.expectEqual(@as(u64, 2), index.write_profile.posting_delta_fold_records);
    try std.testing.expectEqual(@as(u64, 2), index.write_profile.posting_delta_fold_deleted_tail_keys);
    try std.testing.expectEqual(@as(u64, 36), index.write_profile.posting_delta_fold_deleted_tail_key_bytes);
    try std.testing.expect(index.write_profile.posting_delta_fold_deleted_tail_value_bytes > 0);
    try std.testing.expectEqual(@as(u64, 10), index.write_profile.posting_delta_fold_written_base_key_bytes);
    try std.testing.expectEqual(@as(u64, 41), index.write_profile.posting_delta_fold_written_base_value_bytes);
}

test "posting store can defer delta fold until policy threshold is reached" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    const members = [_]VectorId{ 100, 200, 300, 400 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 1,
        .op = .replace,
        .vector_id = 100,
    });

    const skipped = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 2,
    });
    try std.testing.expect(skipped.skipped);
    try std.testing.expectEqual(@as(usize, 1), skipped.delta_records);
    try std.testing.expectEqual(@as(u64, 4), skipped.next_generation);

    var base_after_skip = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_skip.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), base_after_skip.generation);
    const deltas_after_skip = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after_skip);
    try std.testing.expectEqual(@as(usize, 1), deltas_after_skip.len);

    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 2,
        .op = .tombstone,
        .vector_id = 200,
    });
    const folded = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 2,
    });
    try std.testing.expect(!folded.skipped);
    try std.testing.expectEqual(@as(usize, 2), folded.delta_records);
    try std.testing.expectEqual(@as(usize, 10), folded.written_base_key_bytes);
    try std.testing.expectEqual(@as(usize, 49), folded.written_base_value_bytes);
    try std.testing.expectEqual(@as(u64, 5), folded.next_generation);

    var base_after_fold = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_fold.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), base_after_fold.generation);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 100, 300, 400 }, base_after_fold.members);
    const deltas_after_fold = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after_fold);
    try std.testing.expectEqual(@as(usize, 0), deltas_after_fold.len);
}

test "posting store query member copy overlays base and delta tail in shadow mode" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .shadow_base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};
    var scratch = PostingQueryMaterializeTestScratch{};
    defer scratch.deinit(alloc);
    var profile = PostingQueryMaterializeTestProfile{};

    const centroid = [_]f32{ 1.0, 2.0 };
    const base_members = [_]VectorId{ 10, 20 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 1,
        .members = base_members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 2) << 32) | 1,
        .op = .tombstone,
        .vector_id = 10,
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 2) << 32) | 2,
        .op = .insert,
        .vector_id = 30,
    });

    const packed_members = [_]VectorId{ 20, 30 };
    const materialized = try PostingStore.copyQueryMemberIds(
        &index,
        &txn,
        alloc,
        &scratch,
        .{
            .id = 9,
            .parent = 1,
            .level = 0,
            .centroid = centroid[0..],
            .members = packed_members[0..],
            .state = .{},
        },
        &profile,
        postingQueryTestNow,
        postingQueryTestElapsed,
    );

    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 20, 30 }, materialized);
    try std.testing.expectEqual(@as(u64, 1), profile.posting_overlay_calls);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_overlay_base_members);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_overlay_delta_records);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_overlay_materialized_members);
    try std.testing.expectEqual(@as(u64, 0), profile.posting_overlay_fallbacks);
    try std.testing.expectEqual(@as(u64, 15), profile.posting_overlay_ns);
}

test "posting store query member copy skips delta scan when canonical base is current" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{ .dims = 2, .posting_storage_mode = .base_delta },
    };
    defer index.deinit();
    var txn = struct {}{};
    var scratch = PostingQueryMaterializeTestScratch{};
    defer scratch.deinit(alloc);
    var profile = PostingQueryMaterializeTestProfile{};

    const centroid = [_]f32{ 1.0, 2.0 };
    const base_members = [_]VectorId{ 10, 20, 30 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = base_members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 4) << 32) | 1,
        .op = .replace,
        .vector_id = 10,
    });
    index.cursor_open_count = 0;

    const materialized = try PostingStore.copyQueryMemberIds(
        &index,
        &txn,
        alloc,
        &scratch,
        .{
            .id = 9,
            .parent = 1,
            .level = 0,
            .centroid = centroid[0..],
            .members = &.{},
            .state = .{ .mutation_version = 4 },
        },
        &profile,
        postingQueryTestNow,
        postingQueryTestElapsed,
    );

    try std.testing.expectEqualSlices(VectorId, base_members[0..], materialized);
    try std.testing.expectEqual(@as(u64, 0), index.cursor_open_count);
    try std.testing.expectEqual(@as(u64, 1), profile.posting_overlay_calls);
    try std.testing.expectEqual(@as(u64, 3), profile.posting_overlay_base_members);
    try std.testing.expectEqual(@as(u64, 0), profile.posting_overlay_delta_records);
    try std.testing.expectEqual(@as(u64, 1), profile.posting_overlay_delta_scan_skips);
    try std.testing.expectEqual(@as(u64, 3), profile.posting_overlay_materialized_members);
}
