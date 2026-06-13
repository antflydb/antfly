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

pub const PostingBaseHeader = struct {
    posting_id: PostingId,
    generation: u64,
    member_count: usize,
};

pub const PostingBaseStats = struct {
    header: PostingBaseHeader,
    block_count: usize,
    encoded_len: usize,
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

pub const PostingDeltaTailStats = struct {
    records: usize = 0,
    records_after_generation: usize = 0,
    tombstones_after_generation: usize = 0,
    encoded_key_bytes: usize = 0,
    encoded_value_bytes: usize = 0,
    max_sequence_after_generation: u64 = 0,
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
    peak_scratch_bytes: usize = 0,
    next_generation: u64 = 0,
    skipped: bool = false,
};

const DeleteDeltaTailStats = struct {
    keys: usize = 0,
    key_bytes: usize = 0,
    value_bytes: usize = 0,
};

pub const DeltaReplayResult = struct {
    records: usize = 0,
    max_sequence: u64 = 0,
};

pub const FoldDeltaTailOptions = struct {
    min_delta_records: usize = 1,
    min_tombstone_records: usize = 0,
    min_delta_to_base_ratio_bps: u32 = 0,
    min_delta_value_bytes: usize = 0,
    max_materialized_members: usize = std.math.maxInt(usize),
    max_materialized_bytes: usize = std.math.maxInt(usize),
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
        const out = try alloc.alloc(u8, encoded_len);
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
    const delta_compact_single_flag: u8 = 0x80;

    const base_header_size: usize = 4 + 1 + 8 + 8 + 4;
    pub const encoded_base_header_size: usize = base_header_size;
    pub const base_member_default_block_size: usize = 32;
    pub const base_member_max_block_size: usize = 64;
    pub const delta_header_size: usize = 4 + 1 + 4 + 8;
    pub const EncodedBaseResult = struct {
        encoded: []const u8,
        encoded_len: usize = 0,
        member_count: usize,
    };

    pub fn encodedBaseSizeForMemberCount(member_count: usize) !usize {
        if (member_count > std.math.maxInt(u32)) return error.TooLarge;
        const block_count = (member_count + base_member_default_block_size - 1) / base_member_default_block_size;
        return base_header_size +
            try std.math.mul(usize, block_count, 11) +
            try std.math.mul(usize, member_count, 10);
    }

    pub fn encodedBaseSize(base: PostingBase) !usize {
        return try encodedBaseSizeForMembers(base.members);
    }

    pub fn encodedBaseSizeForMembers(members: []const VectorId) !usize {
        return try encodedBaseSizeForMembersWithBlockSize(members, base_member_default_block_size);
    }

    pub fn encodedBaseSizeForMembersWithBlockSize(members: []const VectorId, block_size: usize) !usize {
        if (members.len > std.math.maxInt(u32)) return error.TooLarge;
        var sizer = BaseMemberBlockSizer.init(normalizeBaseMemberBlockSize(block_size));
        try sizer.appendSlice(members);
        return try sizer.finish();
    }

    const BaseMemberBlockSizer = struct {
        total: usize = base_header_size,
        target_block_size: usize = base_member_default_block_size,
        block: [base_member_max_block_size]VectorId = undefined,
        block_count: usize = 0,

        fn init(block_size: usize) BaseMemberBlockSizer {
            return .{ .target_block_size = normalizeBaseMemberBlockSize(block_size) };
        }

        fn append(self: *BaseMemberBlockSizer, member_id: VectorId) !void {
            self.block[self.block_count] = member_id;
            self.block_count += 1;
            if (self.block_count == self.target_block_size) try self.flush();
        }

        fn appendSlice(self: *BaseMemberBlockSizer, members: []const VectorId) !void {
            for (members) |member_id| try self.append(member_id);
        }

        fn flush(self: *BaseMemberBlockSizer) !void {
            if (self.block_count == 0) return;
            const block = self.block[0..self.block_count];
            const block_min = minVectorId(block);
            self.total = try std.math.add(usize, self.total, 1 + varintSize(block_min));
            for (block) |member_id| {
                self.total = try std.math.add(usize, self.total, varintSize(member_id - block_min));
            }
            self.block_count = 0;
        }

        fn finish(self: *BaseMemberBlockSizer) !usize {
            try self.flush();
            return self.total;
        }
    };

    pub fn encodedBaseSizeWithOverlayPlan(scratch: anytype, base_data: []const u8) !EncodedBaseResult {
        return try encodedBaseSizeWithOverlayPlanWithBlockSize(scratch, base_data, base_member_default_block_size);
    }

    pub fn encodedBaseSizeWithOverlayPlanWithBlockSize(scratch: anytype, base_data: []const u8, block_size: usize) !EncodedBaseResult {
        const header = try decodeBaseHeader(base_data);
        var sizer = BaseMemberBlockSizer.init(block_size);
        var reader = BaseMemberBlockReader.init(base_data, header.member_count);
        var member_count: usize = 0;
        while (try reader.next()) |member| {
            if (overlayRemovedMembers(scratch).contains(member)) continue;
            try sizer.append(member);
            member_count += 1;
        }
        try reader.finish();

        const appended_ids = overlayAppendedIds(scratch);
        const appended_live = overlayAppendedLive(scratch);
        const appended_count = overlayAppendedCount(scratch).*;
        var append_index: usize = 0;
        while (append_index < appended_count) : (append_index += 1) {
            if (!appended_live[append_index]) continue;
            try sizer.append(appended_ids[append_index]);
            member_count += 1;
        }
        return .{
            .encoded = &.{},
            .encoded_len = try sizer.finish(),
            .member_count = member_count,
        };
    }

    pub fn encodedSortedBaseSizeWithOverlayPlan(alloc: std.mem.Allocator, scratch: anytype, base_data: []const u8) !EncodedBaseResult {
        const appended = try collectSortedLiveAppended(alloc, scratch);
        return try encodedSortedBaseSizeWithPreparedAppended(base_data, scratch, appended, base_member_default_block_size);
    }

    pub fn encodedSortedBaseSizeWithPreparedAppended(base_data: []const u8, scratch: anytype, appended: []const VectorId, block_size: usize) !EncodedBaseResult {
        const header = try decodeBaseHeader(base_data);
        var sizer = BaseMemberBlockSizer.init(block_size);
        var reader = BaseMemberBlockReader.init(base_data, header.member_count);
        var append_index: usize = 0;
        var member_count: usize = 0;
        while (try reader.next()) |member| {
            if (overlayRemovedMembers(scratch).contains(member)) continue;
            while (append_index < appended.len and appended[append_index] < member) : (append_index += 1) {
                try sizer.append(appended[append_index]);
                member_count += 1;
            }
            if (append_index < appended.len and appended[append_index] == member) append_index += 1;
            try sizer.append(member);
            member_count += 1;
        }
        try reader.finish();
        while (append_index < appended.len) : (append_index += 1) {
            try sizer.append(appended[append_index]);
            member_count += 1;
        }
        return .{
            .encoded = &.{},
            .encoded_len = try sizer.finish(),
            .member_count = member_count,
        };
    }

    pub fn encodeBase(alloc: std.mem.Allocator, base: PostingBase) ![]u8 {
        return try encodeBaseWithBlockSize(alloc, base, base_member_default_block_size);
    }

    pub fn encodeBaseWithBlockSize(alloc: std.mem.Allocator, base: PostingBase, block_size: usize) ![]u8 {
        const encoded_len = try encodedBaseSizeForMembersWithBlockSize(base.members, block_size);
        const out = try alloc.alloc(u8, encoded_len);
        errdefer alloc.free(out);

        encodeBaseHeader(out, base.posting_id, base.generation, base.members.len);
        var writer = BaseMemberBlockWriter.init(out, block_size);
        try writer.appendSlice(base.members);
        _ = try writer.finish();
        return out;
    }

    const BaseMemberBlockWriter = struct {
        out: []u8,
        target_block_size: usize,
        pos: usize = base_header_size,
        block: [base_member_max_block_size]VectorId = undefined,
        block_count: usize = 0,

        fn init(out: []u8, block_size: usize) BaseMemberBlockWriter {
            return .{
                .out = out,
                .target_block_size = normalizeBaseMemberBlockSize(block_size),
            };
        }

        fn append(self: *BaseMemberBlockWriter, member_id: VectorId) !void {
            self.block[self.block_count] = member_id;
            self.block_count += 1;
            if (self.block_count == self.target_block_size) try self.flush();
        }

        fn appendSlice(self: *BaseMemberBlockWriter, members: []const VectorId) !void {
            for (members) |member_id| try self.append(member_id);
        }

        fn flush(self: *BaseMemberBlockWriter) !void {
            if (self.block_count == 0) return;
            if (self.pos >= self.out.len) return error.BufferTooSmall;
            self.out[self.pos] = @intCast(self.block_count);
            self.pos += 1;
            const block_min = minVectorId(self.block[0..self.block_count]);
            writeVarint(self.out, &self.pos, block_min);
            for (self.block[0..self.block_count]) |member_id| {
                writeVarint(self.out, &self.pos, member_id - block_min);
            }
            self.block_count = 0;
        }

        fn finish(self: *BaseMemberBlockWriter) !usize {
            try self.flush();
            return self.pos;
        }
    };

    const BaseMemberBlockReader = struct {
        data: []const u8,
        pos: usize = base_header_size,
        remaining_total: usize,
        remaining_block: usize = 0,
        block_min: VectorId = 0,

        fn init(data: []const u8, member_count: usize) BaseMemberBlockReader {
            return .{
                .data = data,
                .remaining_total = member_count,
            };
        }

        fn next(self: *BaseMemberBlockReader) !?VectorId {
            if (self.remaining_total == 0) return null;
            if (self.remaining_block == 0) {
                self.remaining_block = try readBaseBlockCount(self.data, &self.pos, self.remaining_total);
                self.block_min = try readVarint(self.data, &self.pos);
            }
            const delta = try readVarint(self.data, &self.pos);
            self.remaining_block -= 1;
            self.remaining_total -= 1;
            return std.math.add(VectorId, self.block_min, delta) catch return error.Corrupted;
        }

        fn finish(self: *const BaseMemberBlockReader) !void {
            if (self.remaining_total != 0 or self.remaining_block != 0 or self.pos != self.data.len) return error.Corrupted;
        }
    };

    pub const BaseMemberIterator = struct {
        header: PostingBaseHeader,
        reader: BaseMemberBlockReader,

        pub fn init(data: []const u8) !BaseMemberIterator {
            const header = try decodeBaseHeader(data);
            return .{
                .header = header,
                .reader = BaseMemberBlockReader.init(data, header.member_count),
            };
        }

        pub fn memberCount(self: *const BaseMemberIterator) usize {
            return self.header.member_count;
        }

        pub fn next(self: *BaseMemberIterator) !?VectorId {
            return try self.reader.next();
        }

        pub fn finish(self: *const BaseMemberIterator) !void {
            try self.reader.finish();
        }
    };

    fn readBaseBlockCount(data: []const u8, pos: *usize, remaining_members: usize) !usize {
        if (pos.* >= data.len) return error.Corrupted;
        const block_count = data[pos.*];
        pos.* += 1;
        if (block_count == 0 or block_count > base_member_max_block_size or block_count > remaining_members) return error.Corrupted;
        return block_count;
    }

    pub fn normalizeBaseMemberBlockSize(block_size: usize) usize {
        return switch (block_size) {
            16, 32, 64 => block_size,
            else => base_member_default_block_size,
        };
    }

    fn minVectorId(members: []const VectorId) VectorId {
        var out = members[0];
        for (members[1..]) |member_id| out = @min(out, member_id);
        return out;
    }

    fn encodeBaseHeader(out: []u8, posting_id: PostingId, generation: u64, member_count: usize) void {
        @memcpy(out[0..4], &base_magic);
        out[4] = version;
        std.mem.writeInt(u64, out[5..13], posting_id, .little);
        std.mem.writeInt(u64, out[13..21], generation, .little);
        std.mem.writeInt(u32, out[21..25], @intCast(member_count), .little);
    }

    pub fn initializeEncodedBaseFromBaseData(
        out: []u8,
        base_data: []const u8,
        posting_id: PostingId,
        generation: u64,
    ) !PostingBaseHeader {
        const header = try decodeBaseHeader(base_data);
        if (out.len < base_data.len) return error.BufferTooSmall;
        encodeBaseHeader(out, posting_id, generation, header.member_count);
        @memcpy(out[base_header_size..base_data.len], base_data[base_header_size..]);
        return header;
    }

    pub fn finishEncodedBase(out: []u8, encoded_len: usize, member_count: usize) ![]const u8 {
        if (member_count > std.math.maxInt(u32)) return error.TooLarge;
        if (out.len < encoded_len) return error.BufferTooSmall;
        std.mem.writeInt(u32, out[21..25], @intCast(member_count), .little);
        return out[0..encoded_len];
    }

    pub fn encodeBaseMembersInto(out: []u8, posting_id: PostingId, generation: u64, members: []const VectorId) ![]const u8 {
        return try encodeBaseMembersIntoWithBlockSize(out, posting_id, generation, members, base_member_default_block_size);
    }

    pub fn encodeBaseMembersIntoWithBlockSize(out: []u8, posting_id: PostingId, generation: u64, members: []const VectorId, block_size: usize) ![]const u8 {
        const needed = try encodedBaseSizeForMembersWithBlockSize(members, block_size);
        if (out.len < needed) return error.BufferTooSmall;
        return try encodeBaseMembersKnownSizeIntoWithBlockSize(out, posting_id, generation, members, block_size);
    }

    pub fn encodeBaseMembersKnownSizeIntoWithBlockSize(out: []u8, posting_id: PostingId, generation: u64, members: []const VectorId, block_size: usize) ![]const u8 {
        encodeBaseHeader(out, posting_id, generation, 0);
        var writer = BaseMemberBlockWriter.init(out, block_size);
        try writer.appendSlice(members);
        const output_pos = try writer.finish();
        return try finishEncodedBase(out, output_pos, members.len);
    }

    pub fn encodedSortedBaseSizeWithCompactDeltaRecords(base_data: []const u8, scratch: anytype, block_size: usize) !EncodedBaseResult {
        var sizer = BaseMemberBlockSizer.init(block_size);
        const member_count = try streamSortedBaseWithCompactDeltaRecords(&sizer, base_data, scratch);
        return .{
            .encoded = &.{},
            .encoded_len = try sizer.finish(),
            .member_count = member_count,
        };
    }

    pub fn encodeSortedBaseWithCompactDeltaRecords(
        scratch: anytype,
        base_data: []const u8,
        posting_id: PostingId,
        generation: u64,
        block_size: usize,
    ) !EncodedBaseResult {
        encodeBaseHeader(scratch.encoded_base, posting_id, generation, 0);
        var writer = BaseMemberBlockWriter.init(scratch.encoded_base, block_size);
        const member_count = try streamSortedBaseWithCompactDeltaRecords(&writer, base_data, scratch);
        const output_pos = try writer.finish();
        return .{
            .encoded = try finishEncodedBase(scratch.encoded_base, output_pos, member_count),
            .encoded_len = output_pos,
            .member_count = member_count,
        };
    }

    pub fn materializeSortedBaseWithCompactDeltaRecordsIntoScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        base_data: []const u8,
    ) !usize {
        var base_iter = try BaseMemberIterator.init(base_data);
        stableSortCompactDeltaRecordsByVector(scratch);
        try scratch.ensureMemberIdCapacity(alloc, base_iter.memberCount() + scratch.compactDeltaRecordCount());
        const out = scratch.member_ids;
        var out_count: usize = 0;
        var maybe_base = try base_iter.next();
        var record_index: usize = 0;

        while (maybe_base != null or record_index < scratch.compact_delta_count) {
            if (record_index >= scratch.compact_delta_count) {
                out[out_count] = maybe_base.?;
                out_count += 1;
                maybe_base = try base_iter.next();
                continue;
            }

            const vector_id = scratch.compact_delta_ids[record_index];
            var last_op = scratch.compact_delta_ops[record_index];
            record_index += 1;
            while (record_index < scratch.compact_delta_count and scratch.compact_delta_ids[record_index] == vector_id) : (record_index += 1) {
                last_op = scratch.compact_delta_ops[record_index];
            }

            while (maybe_base) |base_member| {
                if (base_member >= vector_id) break;
                out[out_count] = base_member;
                out_count += 1;
                maybe_base = try base_iter.next();
            }
            const present_in_base = if (maybe_base) |base_member| base_member == vector_id else false;
            if (last_op != .tombstone) {
                out[out_count] = vector_id;
                out_count += 1;
            }
            if (present_in_base) maybe_base = try base_iter.next();
        }
        try base_iter.finish();
        return out_count;
    }

    pub fn stableSortCompactDeltaRecordsByVector(scratch: anytype) void {
        var i: usize = 1;
        while (i < scratch.compact_delta_count) : (i += 1) {
            const id = scratch.compact_delta_ids[i];
            const op = scratch.compact_delta_ops[i];
            var j = i;
            while (j > 0 and scratch.compact_delta_ids[j - 1] > id) : (j -= 1) {
                scratch.compact_delta_ids[j] = scratch.compact_delta_ids[j - 1];
                scratch.compact_delta_ops[j] = scratch.compact_delta_ops[j - 1];
            }
            scratch.compact_delta_ids[j] = id;
            scratch.compact_delta_ops[j] = op;
        }
    }

    pub fn applySortedCompactOpsToSortedScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        base_member_count: usize,
        ids: []VectorId,
        ops: []PostingDeltaOp,
    ) !usize {
        stableSortCompactOpsByVector(ids, ops);
        try scratch.ensurePostingOverlayAppendCapacity(alloc, base_member_count + ids.len);
        const base_members = scratch.member_ids[0..base_member_count];
        const out = overlayAppendedIds(scratch);
        var out_count: usize = 0;
        var base_index: usize = 0;
        var op_index: usize = 0;
        while (base_index < base_members.len or op_index < ids.len) {
            if (op_index >= ids.len) {
                out[out_count] = base_members[base_index];
                out_count += 1;
                base_index += 1;
                continue;
            }
            const vector_id = ids[op_index];
            var last_op = ops[op_index];
            op_index += 1;
            while (op_index < ids.len and ids[op_index] == vector_id) : (op_index += 1) {
                last_op = ops[op_index];
            }
            while (base_index < base_members.len and base_members[base_index] < vector_id) : (base_index += 1) {
                out[out_count] = base_members[base_index];
                out_count += 1;
            }
            const present_in_base = base_index < base_members.len and base_members[base_index] == vector_id;
            if (last_op != .tombstone) {
                out[out_count] = vector_id;
                out_count += 1;
            }
            if (present_in_base) base_index += 1;
        }
        try scratch.ensureMemberIdCapacity(alloc, out_count);
        @memcpy(scratch.member_ids[0..out_count], out[0..out_count]);
        return out_count;
    }

    pub fn stableSortCompactOpsByVector(ids: []VectorId, ops: []PostingDeltaOp) void {
        var i: usize = 1;
        while (i < ids.len) : (i += 1) {
            const id = ids[i];
            const op = ops[i];
            var j = i;
            while (j > 0 and ids[j - 1] > id) : (j -= 1) {
                ids[j] = ids[j - 1];
                ops[j] = ops[j - 1];
            }
            ids[j] = id;
            ops[j] = op;
        }
    }

    fn streamSortedBaseWithCompactDeltaRecords(sink: anytype, base_data: []const u8, scratch: anytype) !usize {
        var base_iter = try BaseMemberIterator.init(base_data);
        var member_count: usize = 0;
        var maybe_base = try base_iter.next();
        var record_index: usize = 0;

        while (maybe_base != null or record_index < scratch.compact_delta_count) {
            if (record_index >= scratch.compact_delta_count) {
                try sink.append(maybe_base.?);
                member_count += 1;
                maybe_base = try base_iter.next();
                continue;
            }

            const vector_id = scratch.compact_delta_ids[record_index];
            var last_op = scratch.compact_delta_ops[record_index];
            record_index += 1;
            while (record_index < scratch.compact_delta_count and scratch.compact_delta_ids[record_index] == vector_id) : (record_index += 1) {
                last_op = scratch.compact_delta_ops[record_index];
            }

            while (maybe_base) |base_member| {
                if (base_member >= vector_id) break;
                try sink.append(base_member);
                member_count += 1;
                maybe_base = try base_iter.next();
            }
            const present_in_base = if (maybe_base) |base_member| base_member == vector_id else false;
            if (last_op != .tombstone) {
                try sink.append(vector_id);
                member_count += 1;
            }
            if (present_in_base) maybe_base = try base_iter.next();
        }
        try base_iter.finish();
        return member_count;
    }

    pub fn encodeBaseWithOverlayPlan(
        scratch: anytype,
        base_data: []const u8,
        posting_id: PostingId,
        generation: u64,
    ) !EncodedBaseResult {
        return try encodeBaseWithOverlayPlanWithBlockSize(scratch, base_data, posting_id, generation, base_member_default_block_size);
    }

    pub fn encodeBaseWithOverlayPlanWithBlockSize(
        scratch: anytype,
        base_data: []const u8,
        posting_id: PostingId,
        generation: u64,
        block_size: usize,
    ) !EncodedBaseResult {
        const header = try decodeBaseHeader(base_data);
        encodeBaseHeader(scratch.encoded_base, posting_id, generation, 0);
        var writer = BaseMemberBlockWriter.init(scratch.encoded_base, block_size);
        var reader = BaseMemberBlockReader.init(base_data, header.member_count);
        var member_count: usize = 0;
        while (try reader.next()) |member| {
            if (overlayRemovedMembers(scratch).contains(member)) continue;
            try writer.append(member);
            member_count += 1;
        }
        try reader.finish();

        const appended_ids = overlayAppendedIds(scratch);
        const appended_live = overlayAppendedLive(scratch);
        const appended_count = overlayAppendedCount(scratch).*;
        var append_index: usize = 0;
        while (append_index < appended_count) : (append_index += 1) {
            if (!appended_live[append_index]) continue;
            try writer.append(appended_ids[append_index]);
            member_count += 1;
        }
        const output_pos = try writer.finish();

        return .{
            .encoded = try finishEncodedBase(scratch.encoded_base, output_pos, member_count),
            .encoded_len = output_pos,
            .member_count = member_count,
        };
    }

    pub fn encodeSortedBaseWithOverlayPlan(
        alloc: std.mem.Allocator,
        scratch: anytype,
        base_data: []const u8,
        posting_id: PostingId,
        generation: u64,
    ) !EncodedBaseResult {
        const appended = try collectSortedLiveAppended(alloc, scratch);
        return try encodeSortedBaseWithPreparedAppended(scratch, base_data, appended, posting_id, generation, base_member_default_block_size);
    }

    pub fn encodeSortedBaseWithPreparedAppended(
        scratch: anytype,
        base_data: []const u8,
        appended: []const VectorId,
        posting_id: PostingId,
        generation: u64,
        block_size: usize,
    ) !EncodedBaseResult {
        const header = try decodeBaseHeader(base_data);
        encodeBaseHeader(scratch.encoded_base, posting_id, generation, 0);
        var writer = BaseMemberBlockWriter.init(scratch.encoded_base, block_size);
        var reader = BaseMemberBlockReader.init(base_data, header.member_count);
        var append_index: usize = 0;
        var member_count: usize = 0;
        while (try reader.next()) |member| {
            if (overlayRemovedMembers(scratch).contains(member)) continue;
            while (append_index < appended.len and appended[append_index] < member) : (append_index += 1) {
                try writer.append(appended[append_index]);
                member_count += 1;
            }
            if (append_index < appended.len and appended[append_index] == member) append_index += 1;
            try writer.append(member);
            member_count += 1;
        }
        try reader.finish();
        while (append_index < appended.len) : (append_index += 1) {
            try writer.append(appended[append_index]);
            member_count += 1;
        }
        const output_pos = try writer.finish();

        return .{
            .encoded = try finishEncodedBase(scratch.encoded_base, output_pos, member_count),
            .encoded_len = output_pos,
            .member_count = member_count,
        };
    }

    pub fn collectSortedLiveAppended(alloc: std.mem.Allocator, scratch: anytype) ![]VectorId {
        const appended_ids = overlayAppendedIds(scratch);
        const appended_live = overlayAppendedLive(scratch);
        const appended_count = overlayAppendedCount(scratch).*;
        var live_count: usize = 0;
        for (appended_live[0..appended_count]) |live| {
            if (live) live_count += 1;
        }
        try scratch.ensureMemberIdCapacity(alloc, live_count);
        var out_count: usize = 0;
        var i: usize = 0;
        while (i < appended_count) : (i += 1) {
            if (!appended_live[i]) continue;
            scratch.member_ids[out_count] = appended_ids[i];
            out_count += 1;
        }
        const out = scratch.member_ids[0..out_count];
        std.mem.sort(VectorId, out, {}, comptime std.sort.asc(VectorId));
        return out;
    }

    pub fn decodeBase(alloc: std.mem.Allocator, data: []const u8) !OwnedPostingBase {
        const header = try decodeBaseHeader(data);
        const members = try alloc.alloc(VectorId, header.member_count);
        errdefer alloc.free(members);
        _ = try decodeBaseMembersInto(data, members);
        return .{
            .posting_id = header.posting_id,
            .generation = header.generation,
            .members = members,
        };
    }

    pub fn decodeBaseHeader(data: []const u8) !PostingBaseHeader {
        if (data.len < base_header_size) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..4], &base_magic)) return error.BadPostingBaseMagic;
        if (data[4] != version) return error.UnsupportedPostingBaseVersion;
        const member_count = std.mem.readInt(u32, data[21..25], .little);
        return .{
            .posting_id = std.mem.readInt(u64, data[5..13], .little),
            .generation = std.mem.readInt(u64, data[13..21], .little),
            .member_count = member_count,
        };
    }

    pub fn decodeBaseStats(data: []const u8) !PostingBaseStats {
        const header = try decodeBaseHeader(data);
        var pos: usize = base_header_size;
        var remaining_members = header.member_count;
        var block_count: usize = 0;
        while (remaining_members != 0) {
            const current_block_count = try readBaseBlockCount(data, &pos, remaining_members);
            const block_min = try readVarint(data, &pos);
            var block_index: usize = 0;
            while (block_index < current_block_count) : (block_index += 1) {
                const delta = try readVarint(data, &pos);
                _ = std.math.add(VectorId, block_min, delta) catch return error.Corrupted;
            }
            remaining_members -= current_block_count;
            block_count += 1;
        }
        if (pos != data.len) return error.Corrupted;
        return .{
            .header = header,
            .block_count = block_count,
            .encoded_len = data.len,
        };
    }

    pub fn validateBase(data: []const u8) !PostingBaseHeader {
        return (try decodeBaseStats(data)).header;
    }

    pub fn decodeBaseMembersInto(data: []const u8, members: []VectorId) !PostingBaseHeader {
        const header = try decodeBaseHeader(data);
        if (members.len < header.member_count) return error.BufferTooSmall;
        var reader = BaseMemberBlockReader.init(data, header.member_count);
        var decoded: usize = 0;
        while (try reader.next()) |member| {
            members[decoded] = member;
            decoded += 1;
        }
        try reader.finish();
        return header;
    }

    pub fn baseContainsSortedMember(data: []const u8, vector_id: VectorId) !bool {
        return try baseContainsSortedMemberWithValidation(data, vector_id, false);
    }

    pub fn baseContainsSortedMemberStrict(data: []const u8, vector_id: VectorId) !bool {
        return try baseContainsSortedMemberWithValidation(data, vector_id, true);
    }

    pub fn baseContainsMemberStrict(data: []const u8, vector_id: VectorId) !bool {
        var iterator = try BaseMemberIterator.init(data);
        var found = false;
        while (try iterator.next()) |member| {
            if (member == vector_id) found = true;
        }
        try iterator.finish();
        return found;
    }

    fn baseContainsSortedMemberWithValidation(data: []const u8, vector_id: VectorId, strict_validation: bool) !bool {
        const header = try decodeBaseHeader(data);
        var pos: usize = base_header_size;
        var remaining_members = header.member_count;
        var found = false;
        var resolved = false;

        while (remaining_members != 0) {
            const current_block_count = try readBaseBlockCount(data, &pos, remaining_members);
            const block_min = try readVarint(data, &pos);
            if (!resolved and block_min > vector_id) {
                if (!strict_validation) return false;
                resolved = true;
            }

            var block_index: usize = 0;
            while (block_index < current_block_count) : (block_index += 1) {
                const delta = try readVarint(data, &pos);
                const member = std.math.add(VectorId, block_min, delta) catch return error.Corrupted;
                if (resolved) continue;
                if (member == vector_id) {
                    if (!strict_validation) return true;
                    found = true;
                    resolved = true;
                } else if (member > vector_id) {
                    if (!strict_validation) return false;
                    resolved = true;
                }
            }
            remaining_members -= current_block_count;
        }
        if (pos != data.len) return error.Corrupted;
        return found;
    }

    pub fn decodeBaseIntoScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        data: []const u8,
    ) !PostingBaseHeader {
        const header = try decodeBaseHeader(data);
        try scratch.ensureMemberIdCapacity(alloc, header.member_count);
        return try decodeBaseMembersInto(data, scratch.member_ids[0..header.member_count]);
    }

    pub fn encodeDeltaTail(alloc: std.mem.Allocator, records: []const PostingDeltaRecord) ![]u8 {
        if (records.len > std.math.maxInt(u32)) return error.TooLarge;
        const encoded_len = try encodedDeltaTailSize(records);
        const out = try alloc.alloc(u8, encoded_len);
        errdefer alloc.free(out);
        return try encodeDeltaTailInto(out, records);
    }

    pub fn encodeDeltaTailInto(out: []u8, records: []const PostingDeltaRecord) ![]u8 {
        if (records.len > std.math.maxInt(u32)) return error.TooLarge;
        const encoded_len = try encodedDeltaTailSize(records);
        if (out.len < encoded_len) return error.BufferTooSmall;
        @memcpy(out[0..4], &delta_magic);
        if (records.len == 1) {
            out[4] = version | delta_compact_single_flag;
            var pos: usize = 5;
            writeVarint(out, &pos, records[0].sequence);
            out[pos] = @intFromEnum(records[0].op);
            pos += 1;
            writeVarint(out, &pos, records[0].vector_id);
            return out[0..encoded_len];
        }
        out[4] = version;
        std.mem.writeInt(u32, out[5..9], @intCast(records.len), .little);

        const base_sequence = baseSequenceForDeltaTail(records);
        std.mem.writeInt(u64, out[9..17], base_sequence, .little);
        var pos: usize = delta_header_size;
        for (records) |record| {
            writeVarint(out, &pos, record.sequence - base_sequence);
            out[pos] = @intFromEnum(record.op);
            pos += 1;
            writeVarint(out, &pos, record.vector_id);
        }
        return out[0..encoded_len];
    }

    pub fn decodeDeltaTail(alloc: std.mem.Allocator, data: []const u8) ![]PostingDeltaRecord {
        var iterator = try DeltaTailIterator.init(data);
        const records = try alloc.alloc(PostingDeltaRecord, iterator.recordCount());
        errdefer alloc.free(records);
        for (records) |*record| {
            record.* = (try iterator.next()) orelse return error.Corrupted;
        }
        if ((try iterator.next()) != null) return error.Corrupted;
        return records;
    }

    pub const DeltaTailHeader = struct {
        record_count: usize,
        base_sequence: u64 = 0,
        records_offset: usize,
        compact_record: ?PostingDeltaRecord = null,
    };

    pub const DeltaTailIterator = struct {
        data: []const u8,
        header: DeltaTailHeader,
        pos: usize,
        index: usize = 0,

        pub fn init(data: []const u8) !DeltaTailIterator {
            const header = try decodeDeltaTailHeader(data);
            return .{
                .data = data,
                .header = header,
                .pos = header.records_offset,
            };
        }

        pub fn recordCount(self: *const DeltaTailIterator) usize {
            return self.header.record_count;
        }

        pub fn next(self: *DeltaTailIterator) !?PostingDeltaRecord {
            if (self.index >= self.header.record_count) {
                if (self.pos != self.data.len) return error.Corrupted;
                return null;
            }
            const record = if (self.header.compact_record) |compact| compact else try readDeltaRecord(self.data, self.header, &self.pos);
            self.index += 1;
            return record;
        }
    };

    fn decodeDeltaTailHeader(data: []const u8) !DeltaTailHeader {
        if (data.len < 5) return error.Corrupted;
        if (!std.mem.eql(u8, data[0..4], &delta_magic)) return error.BadPostingDeltaMagic;
        const raw_version = data[4];
        const compact_single = (raw_version & delta_compact_single_flag) != 0;
        if ((raw_version & ~delta_compact_single_flag) != version) return error.UnsupportedPostingDeltaVersion;
        if (compact_single) {
            var pos: usize = 5;
            const sequence = try readVarint(data, &pos);
            if (pos >= data.len) return error.Corrupted;
            const op = try decodeDeltaOp(data[pos]);
            pos += 1;
            const vector_id = try readVarint(data, &pos);
            if (pos != data.len) return error.Corrupted;
            return .{
                .record_count = 1,
                .records_offset = data.len,
                .compact_record = .{
                    .sequence = sequence,
                    .op = op,
                    .vector_id = vector_id,
                },
            };
        }
        if (data.len < delta_header_size) return error.Corrupted;
        const record_count = std.mem.readInt(u32, data[5..9], .little);
        const header = DeltaTailHeader{
            .record_count = record_count,
            .base_sequence = std.mem.readInt(u64, data[9..17], .little),
            .records_offset = delta_header_size,
        };
        return header;
    }

    pub fn deltaTailRecordCount(data: []const u8) !usize {
        return (try decodeDeltaTailHeader(data)).record_count;
    }

    fn decodeDeltaOp(raw: u8) !PostingDeltaOp {
        return switch (raw) {
            @intFromEnum(PostingDeltaOp.insert) => .insert,
            @intFromEnum(PostingDeltaOp.tombstone) => .tombstone,
            @intFromEnum(PostingDeltaOp.replace) => .replace,
            else => error.UnsupportedPostingDeltaOp,
        };
    }

    fn readDeltaRecord(data: []const u8, header: DeltaTailHeader, pos: *usize) !PostingDeltaRecord {
        const sequence_offset = try readVarint(data, pos);
        if (pos.* >= data.len) return error.Corrupted;
        const op = try decodeDeltaOp(data[pos.*]);
        pos.* += 1;
        const vector_id = try readVarint(data, pos);
        return .{
            .sequence = std.math.add(u64, header.base_sequence, sequence_offset) catch return error.Corrupted,
            .op = op,
            .vector_id = vector_id,
        };
    }

    pub fn deltaTailStatsAfterGeneration(data: []const u8, base_generation: u64) !PostingDeltaTailStats {
        var iterator = try DeltaTailIterator.init(data);
        var stats = PostingDeltaTailStats{
            .records = iterator.recordCount(),
            .encoded_value_bytes = data.len,
        };
        while (try iterator.next()) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            stats.records_after_generation += 1;
            if (record.op == .tombstone) stats.tombstones_after_generation += 1;
            stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
        }
        return stats;
    }

    pub fn deltaTailRecordsAfterGenerationLimited(data: []const u8, base_generation: u64, remaining_limit: *usize) !bool {
        var iterator = try DeltaTailIterator.init(data);
        while (try iterator.next()) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            if (remaining_limit.* == 0) {
                return true;
            } else {
                remaining_limit.* -= 1;
            }
        }
        return false;
    }

    pub fn scanDeltaTailAfterGenerationIntoScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        data: []const u8,
        base_generation: u64,
    ) !PostingDeltaTailStats {
        var iterator = try DeltaTailIterator.init(data);
        try scratch.ensureDeltaRecordCapacity(alloc, scratch.deltaRecordCount() + iterator.recordCount());
        var stats = PostingDeltaTailStats{
            .records = iterator.recordCount(),
            .encoded_value_bytes = data.len,
        };
        while (try iterator.next()) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            stats.records_after_generation += 1;
            if (record.op == .tombstone) stats.tombstones_after_generation += 1;
            stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
            scratch.appendDeltaRecordAssumeCapacity(record);
        }
        return stats;
    }

    pub fn scanDeltaTailAfterGenerationIntoOverlayPlan(
        alloc: std.mem.Allocator,
        scratch: anytype,
        data: []const u8,
        base_generation: u64,
    ) !PostingDeltaTailStats {
        var iterator = try DeltaTailIterator.init(data);
        var stats = PostingDeltaTailStats{
            .records = iterator.recordCount(),
            .encoded_value_bytes = data.len,
        };
        while (try iterator.next()) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            stats.records_after_generation += 1;
            if (record.op == .tombstone) stats.tombstones_after_generation += 1;
            stats.max_sequence_after_generation = @max(stats.max_sequence_after_generation, record.sequence);
            try applyPostingOverlayRecord(alloc, scratch, record);
        }
        return stats;
    }

    pub fn applyPostingOverlayRecord(alloc: std.mem.Allocator, scratch: anytype, record: PostingDeltaRecord) !void {
        removeOverlayAppendedMemberIfPresent(scratch, record.vector_id);
        try overlayRemovedMembers(scratch).put(alloc, record.vector_id, {});
        switch (record.op) {
            .insert, .replace => try appendOverlayLiveMember(alloc, scratch, record.vector_id),
            .tombstone => {},
        }
    }

    fn removeOverlayAppendedMemberIfPresent(scratch: anytype, vector_id: VectorId) void {
        if (overlayAppendedPositions(scratch).fetchRemove(vector_id)) |removed| {
            overlayAppendedLive(scratch)[removed.value] = false;
        }
    }

    fn appendOverlayLiveMember(alloc: std.mem.Allocator, scratch: anytype, vector_id: VectorId) !void {
        try scratch.ensurePostingOverlayAppendCapacity(alloc, overlayAppendedCount(scratch).* + 1);
        const pos = overlayAppendedCount(scratch).*;
        overlayAppendedCount(scratch).* += 1;
        overlayAppendedIds(scratch)[pos] = vector_id;
        overlayAppendedLive(scratch)[pos] = true;
        try overlayAppendedPositions(scratch).put(alloc, vector_id, pos);
    }

    pub fn overlayRemovedMembers(scratch: anytype) *std.AutoHashMapUnmanaged(VectorId, void) {
        const Scratch = std.meta.Child(@TypeOf(scratch));
        if (comptime @hasField(Scratch, "removed_members")) return &scratch.removed_members;
        return &scratch.posting_overlay_removed_members;
    }

    pub fn overlayAppendedPositions(scratch: anytype) *std.AutoHashMapUnmanaged(VectorId, usize) {
        const Scratch = std.meta.Child(@TypeOf(scratch));
        if (comptime @hasField(Scratch, "appended_positions")) return &scratch.appended_positions;
        return &scratch.posting_overlay_appended_positions;
    }

    pub fn overlayAppendedIds(scratch: anytype) []VectorId {
        const Scratch = std.meta.Child(@TypeOf(scratch));
        if (comptime @hasField(Scratch, "appended_ids")) return scratch.appended_ids;
        return scratch.posting_overlay_appended_ids;
    }

    pub fn overlayAppendedLive(scratch: anytype) []bool {
        const Scratch = std.meta.Child(@TypeOf(scratch));
        if (comptime @hasField(Scratch, "appended_live")) return scratch.appended_live;
        return scratch.posting_overlay_appended_live;
    }

    pub fn overlayAppendedCount(scratch: anytype) *usize {
        const Scratch = std.meta.Child(@TypeOf(scratch));
        if (comptime @hasField(Scratch, "appended_count")) return &scratch.appended_count;
        return &scratch.posting_overlay_appended_count;
    }

    pub fn applyDeltaTailAfterGenerationIntoScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        data: []const u8,
        base_generation: u64,
    ) !DeltaReplayResult {
        var iterator = try DeltaTailIterator.init(data);
        try scratch.ensureMemberIdCapacity(alloc, member_count.* + iterator.recordCount());
        var result = DeltaReplayResult{};
        while (try iterator.next()) |record| {
            if (deltaSequenceGeneration(record.sequence) <= base_generation) continue;
            applyDeltaRecordToScratch(scratch, member_count, record);
            result.records += 1;
            result.max_sequence = @max(result.max_sequence, record.sequence);
        }
        return result;
    }

    fn baseSequenceForDeltaTail(records: []const PostingDeltaRecord) u64 {
        if (records.len == 0) return 0;
        var base_sequence = records[0].sequence;
        for (records) |record| {
            base_sequence = @min(base_sequence, record.sequence);
        }
        return base_sequence;
    }

    pub fn encodedDeltaTailSize(records: []const PostingDeltaRecord) !usize {
        if (records.len == 1) {
            return 4 + 1 +
                varintSize(records[0].sequence) +
                1 +
                varintSize(records[0].vector_id);
        }
        var total: usize = delta_header_size;
        const base_sequence = baseSequenceForDeltaTail(records);
        for (records) |record| {
            total = try std.math.add(usize, total, varintSize(record.sequence - base_sequence));
            total = try std.math.add(usize, total, 1);
            total = try std.math.add(usize, total, varintSize(record.vector_id));
        }
        return total;
    }

    pub fn varintSize(value: u64) usize {
        var remaining = value;
        var bytes: usize = 1;
        while (remaining >= 0x80) : (bytes += 1) {
            remaining >>= 7;
        }
        return bytes;
    }

    fn writeVarint(out: []u8, pos: *usize, value: u64) void {
        var remaining = value;
        while (remaining >= 0x80) {
            out[pos.*] = @as(u8, @intCast(remaining & 0x7f)) | 0x80;
            pos.* += 1;
            remaining >>= 7;
        }
        out[pos.*] = @intCast(remaining);
        pos.* += 1;
    }

    fn readVarint(data: []const u8, pos: *usize) !u64 {
        if (pos.* >= data.len) return error.Corrupted;
        const first = data[pos.*];
        pos.* += 1;
        if ((first & 0x80) == 0) return first;
        if (pos.* >= data.len) return error.Corrupted;
        const second = data[pos.*];
        pos.* += 1;
        if ((second & 0x80) == 0) {
            return @as(u64, first & 0x7f) | (@as(u64, second) << 7);
        }
        var out: u64 = 0;
        out |= @as(u64, first & 0x7f);
        out |= @as(u64, second & 0x7f) << 7;
        var shift: u6 = 14;
        var i: usize = 2;
        while (i < 10) : (i += 1) {
            if (pos.* >= data.len) return error.Corrupted;
            const byte = data[pos.*];
            pos.* += 1;
            if (i == 9 and byte > 1) return error.Corrupted;
            out |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return out;
            shift += 7;
        }
        return error.Corrupted;
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

    pub fn applyDeltaRecordToScratch(scratch: anytype, member_count: *usize, record: PostingDeltaRecord) void {
        switch (record.op) {
            .insert, .replace => {
                removeMemberFromScratch(scratch, member_count, record.vector_id);
                scratch.member_ids[member_count.*] = record.vector_id;
                member_count.* += 1;
            },
            .tombstone => removeMemberFromScratch(scratch, member_count, record.vector_id),
        }
    }

    fn removeMemberFromScratch(scratch: anytype, member_count: *usize, vector_id: VectorId) void {
        var i: usize = 0;
        while (i < member_count.*) : (i += 1) {
            if (scratch.member_ids[i] == vector_id) {
                if (i + 1 < member_count.*) {
                    std.mem.copyForwards(
                        VectorId,
                        scratch.member_ids[i .. member_count.* - 1],
                        scratch.member_ids[i + 1 .. member_count.*],
                    );
                }
                member_count.* -= 1;
                return;
            }
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
    min_delta_value_bytes_to_fold: usize = 0,
    max_delta_tail_postings: usize = std.math.maxInt(usize),
    max_delta_fold_materialized_members: usize = std.math.maxInt(usize),
    max_delta_fold_materialized_bytes: usize = std.math.maxInt(usize),
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
    delta_fold_peak_scratch_bytes: u64 = 0,
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
    max_delta_tail_key_bytes: u64 = 0,
    max_delta_tail_value_bytes: u64 = 0,
    max_delta_tail_sequence: u64 = 0,
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
            "posting_backlog scanned_nodes={d} scanned_postings={d} dirty_postings={d} centroid_dirty_postings={d} payload_dirty_postings={d} min_dirty_mutation_version={d} max_dirty_version_age={d} delta_tail_postings={d} max_delta_tail_records={d} max_tombstone_tail_records={d} max_delta_tail_key_bytes={d} max_delta_tail_value_bytes={d} max_delta_tail_sequence={d} max_delta_to_base_ratio_bps={d} overfull_postings={d} postings_at_capacity={d} max_over_capacity_members={d} max_centroid_version_lag={d} max_payload_version_lag={d} max_mutation_version={d} skipped_missing={d}\n",
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
                self.max_delta_tail_key_bytes,
                self.max_delta_tail_value_bytes,
                self.max_delta_tail_sequence,
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
    const overlay_plan_min_delta_records: usize = 8;
    const compact_sorted_delta_max_records: usize = 64;
    const delta_tail_prefetch_posting_count: usize = 3;

    fn IndexType(comptime T: type) type {
        return switch (@typeInfo(T)) {
            .pointer => |ptr| ptr.child,
            else => T,
        };
    }

    fn postingBackend(index: anytype) types.HBCConfig.PostingBackend {
        const Index = IndexType(@TypeOf(index));
        if (comptime @hasField(Index, "config") and @hasField(@TypeOf(index.config), "posting_backend")) {
            return index.config.posting_backend;
        }
        return .lsm;
    }

    fn useSegmentPostingBackend(index: anytype) bool {
        return postingBackend(index) == .segments;
    }

    const OwnedMemberScratch = struct {
        member_ids: []VectorId = &.{},

        fn ensureMemberIdCapacity(self: *OwnedMemberScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
        }

        fn deinit(self: *OwnedMemberScratch, alloc: std.mem.Allocator) void {
            alloc.free(self.member_ids);
            self.member_ids = &.{};
        }
    };

    pub const FoldScratch = struct {
        delta_records: []PostingDeltaRecord = &.{},
        delta_record_count: usize = 0,
        compact_delta_ids: []VectorId = &.{},
        compact_delta_ops: []PostingDeltaOp = &.{},
        compact_delta_count: usize = 0,
        encoded_base: []u8 = &.{},
        member_ids: []VectorId = &.{},
        removed_members: std.AutoHashMapUnmanaged(VectorId, void) = .empty,
        appended_positions: std.AutoHashMapUnmanaged(VectorId, usize) = .empty,
        appended_ids: []VectorId = &.{},
        appended_live: []bool = &.{},
        appended_count: usize = 0,

        pub fn ensureDeltaRecordCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.delta_records.len < needed) self.delta_records = try alloc.realloc(self.delta_records, needed);
        }

        pub fn deltaRecordCount(self: *const FoldScratch) usize {
            return self.delta_record_count;
        }

        pub fn appendDeltaRecordAssumeCapacity(self: *FoldScratch, record: PostingDeltaRecord) void {
            self.delta_records[self.delta_record_count] = record;
            self.delta_record_count += 1;
        }

        pub fn deltaRecords(self: *const FoldScratch) []const PostingDeltaRecord {
            return self.delta_records[0..self.delta_record_count];
        }

        pub fn deltaRecordsMut(self: *FoldScratch) []PostingDeltaRecord {
            return self.delta_records[0..self.delta_record_count];
        }

        pub fn resetDeltaRecords(self: *FoldScratch) void {
            self.delta_record_count = 0;
        }

        pub fn ensureCompactDeltaCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.compact_delta_ids.len < needed) self.compact_delta_ids = try alloc.realloc(self.compact_delta_ids, needed);
            if (self.compact_delta_ops.len < needed) self.compact_delta_ops = try alloc.realloc(self.compact_delta_ops, needed);
        }

        pub fn compactDeltaRecordCount(self: *const FoldScratch) usize {
            return self.compact_delta_count;
        }

        pub fn appendCompactDeltaRecordAssumeCapacity(self: *FoldScratch, record: PostingDeltaRecord) void {
            self.compact_delta_ids[self.compact_delta_count] = record.vector_id;
            self.compact_delta_ops[self.compact_delta_count] = record.op;
            self.compact_delta_count += 1;
        }

        pub fn resetCompactDeltaRecords(self: *FoldScratch) void {
            self.compact_delta_count = 0;
        }

        pub fn ensureEncodedBaseCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.encoded_base.len < needed) self.encoded_base = try alloc.realloc(self.encoded_base, needed);
        }

        pub fn ensureMemberIdCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
        }

        pub fn ensureAppendCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            if (self.appended_ids.len < needed) self.appended_ids = try alloc.realloc(self.appended_ids, needed);
            if (self.appended_live.len < needed) self.appended_live = try alloc.realloc(self.appended_live, needed);
        }

        pub fn ensurePostingOverlayAppendCapacity(self: *FoldScratch, alloc: std.mem.Allocator, needed: usize) !void {
            try self.ensureAppendCapacity(alloc, needed);
        }

        pub fn resetFoldApply(self: *FoldScratch) void {
            self.resetDeltaRecords();
            self.resetCompactDeltaRecords();
            self.removed_members.clearRetainingCapacity();
            self.appended_positions.clearRetainingCapacity();
            self.appended_count = 0;
        }

        pub fn resetPostingOverlayApply(self: *FoldScratch) void {
            self.resetFoldApply();
        }

        pub fn bytes(self: *const FoldScratch) u64 {
            return byteLen(self.delta_records) +
                byteLen(self.compact_delta_ids) +
                byteLen(self.compact_delta_ops) +
                byteLen(self.encoded_base) +
                byteLen(self.member_ids) +
                approximateHashMapBytes(self.removed_members.capacity(), @sizeOf(VectorId), 0) +
                approximateHashMapBytes(self.appended_positions.capacity(), @sizeOf(VectorId), @sizeOf(usize)) +
                byteLen(self.appended_ids) +
                byteLen(self.appended_live);
        }

        pub fn deinit(self: *FoldScratch, alloc: std.mem.Allocator) void {
            alloc.free(self.delta_records);
            alloc.free(self.compact_delta_ids);
            alloc.free(self.compact_delta_ops);
            alloc.free(self.encoded_base);
            alloc.free(self.member_ids);
            self.removed_members.deinit(alloc);
            self.appended_positions.deinit(alloc);
            alloc.free(self.appended_ids);
            alloc.free(self.appended_live);
            self.* = .{};
        }
    };

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
    ) ![]const VectorId {
        if (!shouldMaterializeBaseDeltaForQuery(index, @TypeOf(txn))) {
            return try copyMemberIds(alloc, scratch, posting_view);
        }

        const start = now_fn();
        const canonical_base_delta = baseDeltaIsCanonical(index);
        var owned_base_data: ?[]u8 = null;
        defer if (owned_base_data) |data| index.alloc.free(data);
        const base_data = if (useSegmentPostingBackend(index)) base_data: {
            const Index = IndexType(@TypeOf(index));
            if (comptime !@hasDecl(Index, "loadPostingBackendBaseData")) return error.UnsupportedPostingBackend;
            owned_base_data = index.loadPostingBackendBaseData(txn, posting_view.id, isNotFound) catch |err| {
                if (isNotFound(err)) {
                    notePostingOverlayFallback(profile);
                    return try copyMemberIds(alloc, scratch, posting_view);
                }
                return err;
            };
            break :base_data owned_base_data.?;
        } else loadBaseData(index, txn, posting_view.id, isNotFound) catch |err| {
            if (isNotFound(err)) {
                notePostingOverlayFallback(profile);
                return try copyMemberIds(alloc, scratch, posting_view);
            }
            return err;
        };
        const base_header = try decodeBaseHeaderCachedIfAvailable(scratch, posting_view.id, base_data);
        if (!canonical_base_delta and base_header.generation < posting_view.state.mutation_version) {
            notePostingOverlayFallback(profile);
            return try copyMemberIds(alloc, scratch, posting_view);
        }
        if (canonical_base_delta and base_header.generation >= posting_view.state.mutation_version) {
            if (copyCachedPostingMembersIfAvailable(scratch, posting_view, base_header.generation, 0, profile)) |cached_member_ids| {
                notePostingOverlay(profile, elapsed_fn(start), cached_member_ids.len, 0, cached_member_ids.len);
                return cached_member_ids;
            }
            notePostingOverlayCacheMiss(profile);
            try notePostingMemberCacheMissIfAvailable(scratch, alloc, posting_view.id);

            const decode_start = now_fn();
            _ = try PostingFormat.decodeBaseIntoScratch(alloc, scratch, base_data);
            notePostingBaseDecode(profile, elapsed_fn(decode_start), base_header.member_count);
            const member_ids = scratch.member_ids[0..base_header.member_count];
            try cachePostingMembersIfAvailable(scratch, alloc, posting_view, base_header.generation, 0, member_ids, profile);
            notePostingOverlayDeltaScanSkip(profile);
            notePostingOverlay(profile, elapsed_fn(start), base_header.member_count, 0, base_header.member_count);
            return member_ids;
        }

        const decode_start = now_fn();
        _ = try PostingFormat.decodeBaseIntoScratch(alloc, scratch, base_data);
        notePostingBaseDecode(profile, elapsed_fn(decode_start), base_header.member_count);
        var materialized_len = base_header.member_count;
        const delta_replay_start = now_fn();
        const delta_replay = if (canUsePostingOverlayPlan(@TypeOf(scratch)))
            if (canonical_base_delta)
                try applyDeltaTailIntoScratchAdaptiveSorted(index, txn, posting_view.id, alloc, scratch, &materialized_len, base_header.generation)
            else
                try applyDeltaTailIntoScratchAdaptive(index, txn, posting_view.id, alloc, scratch, &materialized_len, base_header.generation)
        else
            try applyDeltaTailIntoScratch(index, txn, posting_view.id, alloc, scratch, &materialized_len, base_header.generation);
        notePostingDeltaReplay(profile, elapsed_fn(delta_replay_start), delta_replay.records);
        const materialized = scratch.member_ids[0..materialized_len];
        if (!canonical_base_delta and !std.mem.eql(VectorId, materialized, posting_view.members)) {
            notePostingOverlayFallback(profile);
            return try copyMemberIds(alloc, scratch, posting_view);
        }

        if (canonical_base_delta) {
            try cachePostingMembersIfAvailable(scratch, alloc, posting_view, base_header.generation, delta_replay.max_sequence, materialized, profile);
        }
        notePostingOverlay(profile, elapsed_fn(start), base_header.member_count, delta_replay.records, materialized.len);
        return materialized;
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

        var removed_set = if (shouldHashRemovedMembers(node.members.len, vector_ids.len)) blk: {
            var set = std.AutoHashMapUnmanaged(VectorId, void).empty;
            errdefer set.deinit(alloc);
            try set.ensureTotalCapacity(alloc, @intCast(vector_ids.len));
            for (vector_ids) |vector_id| try set.put(alloc, vector_id, {});
            break :blk set;
        } else null;
        defer if (removed_set) |*set| set.deinit(alloc);

        var kept = try alloc.alloc(u64, node.members.len);
        errdefer alloc.free(kept);
        var kept_count: usize = 0;
        var removed_count: usize = 0;
        for (node.members) |member_id| {
            const should_remove = if (removed_set) |*set| set.contains(member_id) else containsMember(vector_ids, member_id);
            if (should_remove) {
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

    fn encodeBaseForIndex(index: anytype, base: PostingBase) ![]u8 {
        const base_member_block_size = postingBaseMemberBlockSize(index);
        if (!shouldSortBaseMembers(index)) return try PostingFormat.encodeBaseWithBlockSize(index.alloc, base, base_member_block_size);
        const sorted_members = try index.alloc.dupe(VectorId, base.members);
        defer index.alloc.free(sorted_members);
        sortVectorIdsAsc(sorted_members);
        const selected_block_size = try postingBaseMemberBlockSizeForMembers(index, sorted_members);
        return try PostingFormat.encodeBaseWithBlockSize(index.alloc, .{
            .posting_id = base.posting_id,
            .generation = base.generation,
            .members = sorted_members,
        }, selected_block_size.block_size);
    }

    pub fn saveBase(index: anytype, txn: anytype, base: PostingBase) !void {
        const encoded = try encodeBaseForIndex(index, base);
        defer index.alloc.free(encoded);
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "savePostingBackendBase")) {
                try index.savePostingBackendBase(txn, base.posting_id, encoded);
                notePostingBasePut(index, 0, encoded);
                return;
            }
            return error.UnsupportedPostingBackend;
        }
        var key_buf: [10]u8 = undefined;
        const key = hbc.encodePostingBaseKey(&key_buf, base.posting_id);
        try index.putNamespaced(txn, .nodes, key, encoded);
        notePostingBasePut(index, key.len, encoded);
    }

    pub fn saveCentroidDirectoryRecord(index: anytype, txn: anytype, record: CentroidDirectoryRecord) !void {
        const encoded = try CentroidDirectoryFormat.encode(index.alloc, record);
        defer index.alloc.free(encoded);
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "savePostingBackendCentroidDirectory")) {
                try index.savePostingBackendCentroidDirectory(txn, record.posting_id, encoded);
                noteCentroidDirectoryPut(index, 0, encoded.len);
                return;
            }
            return error.UnsupportedPostingBackend;
        }
        var key_buf: [10]u8 = undefined;
        const key = hbc.encodeCentroidDirectoryKey(&key_buf, record.posting_id);
        try index.putNamespaced(txn, .nodes, key, encoded);
        noteCentroidDirectoryPut(index, key.len, encoded.len);
    }

    pub fn saveBaseAndCentroidDirectoryRecord(index: anytype, txn: anytype, base: PostingBase, record: CentroidDirectoryRecord) !void {
        const encoded_base = try encodeBaseForIndex(index, base);
        defer index.alloc.free(encoded_base);
        const encoded_centroid = try CentroidDirectoryFormat.encode(index.alloc, record);
        defer index.alloc.free(encoded_centroid);

        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "savePostingBackendBaseAndCentroidDirectory")) {
                try index.savePostingBackendBaseAndCentroidDirectory(txn, base.posting_id, encoded_base, record.posting_id, encoded_centroid);
                notePostingBasePut(index, 0, encoded_base);
                noteCentroidDirectoryPut(index, 0, encoded_centroid.len);
                return;
            }
            if (comptime @hasDecl(Index, "savePostingBackendBase") and @hasDecl(Index, "savePostingBackendCentroidDirectory")) {
                try index.savePostingBackendBase(txn, base.posting_id, encoded_base);
                try index.savePostingBackendCentroidDirectory(txn, record.posting_id, encoded_centroid);
                notePostingBasePut(index, 0, encoded_base);
                noteCentroidDirectoryPut(index, 0, encoded_centroid.len);
                return;
            }
            return error.UnsupportedPostingBackend;
        }

        var base_key_buf: [10]u8 = undefined;
        const base_key = hbc.encodePostingBaseKey(&base_key_buf, base.posting_id);
        var centroid_key_buf: [10]u8 = undefined;
        const centroid_key = hbc.encodeCentroidDirectoryKey(&centroid_key_buf, record.posting_id);
        try index.putNamespaced(txn, .nodes, base_key, encoded_base);
        try index.putNamespaced(txn, .nodes, centroid_key, encoded_centroid);
        notePostingBasePut(index, base_key.len, encoded_base);
        noteCentroidDirectoryPut(index, centroid_key.len, encoded_centroid.len);
    }

    pub fn loadCentroidDirectoryRecord(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedCentroidDirectoryRecord {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendCentroidDirectoryRecord")) {
                return index.loadPostingBackendCentroidDirectoryRecord(txn, posting_id, is_not_found);
            }
            return error.UnsupportedPostingBackend;
        }
        var key_buf: [10]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodeCentroidDirectoryKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        return try CentroidDirectoryFormat.decode(index.alloc, data);
    }

    pub fn loadCentroidDirectoryRecords(index: anytype, txn: anytype) ![]OwnedCentroidDirectoryRecord {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendCentroidDirectoryRecords")) {
                return index.loadPostingBackendCentroidDirectoryRecords(txn);
            }
            return error.UnsupportedPostingBackend;
        }
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

    pub fn loadBaseData(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]const u8 {
        if (useSegmentPostingBackend(index)) return error.UnsupportedPostingBackendBorrowedData;
        var key_buf: [10]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodePostingBaseKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        return data;
    }

    pub fn loadBase(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedPostingBase {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendBase")) {
                return index.loadPostingBackendBase(txn, posting_id, is_not_found);
            }
            return error.UnsupportedPostingBackend;
        }
        const data = try loadBaseData(index, txn, posting_id, is_not_found);
        return try PostingFormat.decodeBase(index.alloc, data);
    }

    pub fn loadBaseHeader(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingBaseHeader {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendBaseHeader")) {
                return index.loadPostingBackendBaseHeader(txn, posting_id, is_not_found);
            }
            return error.UnsupportedPostingBackend;
        }
        const data = try loadBaseData(index, txn, posting_id, is_not_found);
        return try PostingFormat.decodeBaseHeader(data);
    }

    pub fn loadBaseStats(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingBaseStats {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendBaseStats")) {
                return index.loadPostingBackendBaseStats(txn, posting_id, is_not_found);
            }
            return error.UnsupportedPostingBackend;
        }
        const data = try loadBaseData(index, txn, posting_id, is_not_found);
        return try PostingFormat.decodeBaseStats(data);
    }

    pub fn containsBaseDeltaMember(index: anytype, txn: anytype, posting_id: PostingId, vector_id: VectorId, is_not_found: fn (anyerror) bool) !bool {
        if (!baseDeltaIsCanonical(index)) return error.UnsupportedPostingMode;
        var owned_base_data: ?[]u8 = null;
        defer if (owned_base_data) |data| index.alloc.free(data);
        const base_data = if (useSegmentPostingBackend(index)) base_data: {
            const Index = IndexType(@TypeOf(index));
            if (comptime !@hasDecl(Index, "loadPostingBackendBaseData")) return error.UnsupportedPostingBackend;
            owned_base_data = index.loadPostingBackendBaseData(txn, posting_id, is_not_found) catch |err| {
                if (is_not_found(err)) return error.NotFound;
                return err;
            };
            break :base_data owned_base_data.?;
        } else loadBaseData(index, txn, posting_id, is_not_found) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        const base_header = try PostingFormat.decodeBaseHeader(base_data);
        const present_in_base = if (shouldSortBaseMembers(index))
            try PostingFormat.baseContainsSortedMemberStrict(base_data, vector_id)
        else
            try PostingFormat.baseContainsMemberStrict(base_data, vector_id);
        const latest_delta_op = try latestDeltaOpAfterGenerationForMember(index, txn, posting_id, vector_id, base_header.generation);
        return if (latest_delta_op) |op| op != .tombstone else present_in_base;
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
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "deletePostingBackendDeltaTail")) {
                _ = try index.deletePostingBackendDeltaTail(txn, posting_id);
            }
            return;
        }
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
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "appendPostingBackendDeltaRecords")) {
                try index.appendPostingBackendDeltaRecords(txn, posting_id, records);
                notePostingDeltaAppend(index, 0, 0, records.len);
                return;
            }
            return error.UnsupportedPostingBackend;
        }
        const max_value_bytes = maxPostingDeltaTailValueBytes(index);
        var encoded_scratch: []u8 = &.{};
        defer index.alloc.free(encoded_scratch);
        if (max_value_bytes != 0 and records.len > 1) {
            var start: usize = 0;
            while (start < records.len) {
                const end = deltaRecordChunkEnd(records, start, max_value_bytes);
                try appendDeltaRecordChunk(index, txn, posting_id, records[start..end], &encoded_scratch);
                start = end;
            }
            return;
        }
        try appendDeltaRecordChunk(index, txn, posting_id, records, &encoded_scratch);
    }

    fn maxPostingDeltaTailValueBytes(index: anytype) usize {
        if (comptime @hasField(@TypeOf(index.config), "max_posting_delta_tail_value_bytes")) {
            return index.config.max_posting_delta_tail_value_bytes;
        }
        return 0;
    }

    fn deltaRecordChunkEnd(records: []const PostingDeltaRecord, start: usize, max_value_bytes: usize) usize {
        const base_sequence = records[start].sequence;
        var encoded_bytes: usize = 0;
        var end = start;
        while (end < records.len) {
            const record = records[end];
            if (record.sequence < base_sequence and end != start) break;
            const sequence_offset = record.sequence - base_sequence;
            const record_bytes = PostingFormat.varintSize(sequence_offset) + 1 + PostingFormat.varintSize(record.vector_id);
            const candidate_records = end - start + 1;
            const candidate_bytes = if (candidate_records == 1)
                4 + 1 + PostingFormat.varintSize(record.sequence) + 1 + PostingFormat.varintSize(record.vector_id)
            else
                PostingFormat.delta_header_size + encoded_bytes + record_bytes;
            if (end != start and candidate_bytes > max_value_bytes) break;
            encoded_bytes += record_bytes;
            end += 1;
        }
        return end;
    }

    fn appendDeltaRecordChunk(index: anytype, txn: anytype, posting_id: PostingId, records: []const PostingDeltaRecord, encoded_scratch: *[]u8) !void {
        var key_buf: [18]u8 = undefined;
        const encoded_len = try PostingFormat.encodedDeltaTailSize(records);
        if (encoded_scratch.len < encoded_len) {
            encoded_scratch.* = try index.alloc.realloc(encoded_scratch.*, encoded_len);
        }
        const encoded = try PostingFormat.encodeDeltaTailInto(encoded_scratch.*, records);
        const key = hbc.encodePostingDeltaKey(&key_buf, posting_id, records[0].sequence);
        try appendNamespaced(index, txn, .nodes, key, encoded);
        notePostingDeltaAppend(index, key.len, encoded.len, records.len);
    }

    pub fn loadDeltaTail(index: anytype, txn: anytype, posting_id: PostingId) ![]PostingDeltaRecord {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendDeltaTail")) {
                return index.loadPostingBackendDeltaTail(txn, posting_id);
            }
            return error.UnsupportedPostingBackend;
        }
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

    pub fn deltaTailStats(index: anytype, txn: anytype, posting_id: PostingId, base_generation: u64) !PostingDeltaTailStats {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "postingBackendDeltaTailStats")) {
                return index.postingBackendDeltaTailStats(txn, posting_id, base_generation);
            }
            return error.UnsupportedPostingBackend;
        }
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var out = PostingDeltaTailStats{};

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            const stats = try PostingFormat.deltaTailStatsAfterGeneration(entry.value, base_generation);
            out.records += stats.records;
            out.records_after_generation += stats.records_after_generation;
            out.tombstones_after_generation += stats.tombstones_after_generation;
            out.max_sequence_after_generation = @max(out.max_sequence_after_generation, stats.max_sequence_after_generation);
            out.encoded_key_bytes += entry.key.len;
            out.encoded_value_bytes += entry.value.len;
            maybe_entry = try cursor.next();
        }
        return out;
    }

    fn latestDeltaOpAfterGenerationForMember(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        vector_id: VectorId,
        base_generation: u64,
    ) !?PostingDeltaOp {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "postingBackendLatestDeltaOpAfterGenerationForMember")) {
                return try index.postingBackendLatestDeltaOpAfterGenerationForMember(txn, posting_id, vector_id, base_generation);
            }
            return error.UnsupportedPostingBackend;
        }
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var best_sequence: u64 = 0;
        var best_op: ?PostingDeltaOp = null;

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                if (record.vector_id != vector_id) continue;
                if (PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                if (best_op == null or record.sequence >= best_sequence) {
                    best_sequence = record.sequence;
                    best_op = record.op;
                }
            }
            maybe_entry = try cursor.next();
        }
        return best_op;
    }

    const AdaptiveDeltaTailScan = struct {
        stats: PostingDeltaTailStats = .{},
        use_overlay_plan: bool = false,
    };

    fn scanDeltaTailAdaptiveForFold(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        alloc: std.mem.Allocator,
        scratch: *FoldScratch,
        base_generation: u64,
        prefer_compact_records: bool,
    ) !AdaptiveDeltaTailScan {
        scratch.resetPostingOverlayApply();
        scratch.resetDeltaRecords();
        scratch.resetCompactDeltaRecords();
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var out = AdaptiveDeltaTailScan{};
        var buffered: [overlay_plan_min_delta_records]PostingDeltaRecord = undefined;
        var buffered_count: usize = 0;

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            out.stats.records += iterator.recordCount();
            out.stats.encoded_key_bytes += entry.key.len;
            out.stats.encoded_value_bytes += entry.value.len;
            while (try iterator.next()) |record| {
                if (PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                out.stats.records_after_generation += 1;
                if (record.op == .tombstone) out.stats.tombstones_after_generation += 1;
                out.stats.max_sequence_after_generation = @max(out.stats.max_sequence_after_generation, record.sequence);
                if (prefer_compact_records and !out.use_overlay_plan) {
                    if (scratch.compactDeltaRecordCount() < compact_sorted_delta_max_records) {
                        try scratch.ensureCompactDeltaCapacity(alloc, scratch.compactDeltaRecordCount() + 1);
                        scratch.appendCompactDeltaRecordAssumeCapacity(record);
                        continue;
                    }
                    out.use_overlay_plan = true;
                    var compact_index: usize = 0;
                    while (compact_index < scratch.compactDeltaRecordCount()) : (compact_index += 1) {
                        try PostingFormat.applyPostingOverlayRecord(alloc, scratch, .{
                            .sequence = 0,
                            .op = scratch.compact_delta_ops[compact_index],
                            .vector_id = scratch.compact_delta_ids[compact_index],
                        });
                    }
                    scratch.resetCompactDeltaRecords();
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                    continue;
                }
                if (out.use_overlay_plan) {
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                } else if (buffered_count < buffered.len) {
                    buffered[buffered_count] = record;
                    buffered_count += 1;
                } else {
                    out.use_overlay_plan = true;
                    var i: usize = 0;
                    while (i < buffered_count) : (i += 1) {
                        try PostingFormat.applyPostingOverlayRecord(alloc, scratch, buffered[i]);
                    }
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                }
            }
            maybe_entry = try cursor.next();
        }
        if (!out.use_overlay_plan and buffered_count != 0) {
            try scratch.ensureDeltaRecordCapacity(alloc, buffered_count);
            for (buffered[0..buffered_count]) |record| scratch.appendDeltaRecordAssumeCapacity(record);
        }
        return out;
    }

    fn stableSortCompactDeltaRecordsByVector(scratch: *FoldScratch) void {
        var i: usize = 1;
        while (i < scratch.compact_delta_count) : (i += 1) {
            const id = scratch.compact_delta_ids[i];
            const op = scratch.compact_delta_ops[i];
            var j = i;
            while (j > 0 and scratch.compact_delta_ids[j - 1] > id) : (j -= 1) {
                scratch.compact_delta_ids[j] = scratch.compact_delta_ids[j - 1];
                scratch.compact_delta_ops[j] = scratch.compact_delta_ops[j - 1];
            }
            scratch.compact_delta_ids[j] = id;
            scratch.compact_delta_ops[j] = op;
        }
    }

    fn materializeSortedBaseWithCompactDeltaRecords(
        alloc: std.mem.Allocator,
        scratch: *FoldScratch,
        base_data: []const u8,
    ) !usize {
        var base_iter = try PostingFormat.BaseMemberIterator.init(base_data);
        stableSortCompactDeltaRecordsByVector(scratch);
        try scratch.ensureMemberIdCapacity(alloc, base_iter.memberCount() + scratch.compactDeltaRecordCount());
        const out = scratch.member_ids;
        var out_count: usize = 0;
        var maybe_base = try base_iter.next();
        var record_index: usize = 0;

        while (maybe_base != null or record_index < scratch.compact_delta_count) {
            if (record_index >= scratch.compact_delta_count) {
                out[out_count] = maybe_base.?;
                out_count += 1;
                maybe_base = try base_iter.next();
                continue;
            }

            const vector_id = scratch.compact_delta_ids[record_index];
            var last_op = scratch.compact_delta_ops[record_index];
            record_index += 1;
            while (record_index < scratch.compact_delta_count and scratch.compact_delta_ids[record_index] == vector_id) : (record_index += 1) {
                last_op = scratch.compact_delta_ops[record_index];
            }

            while (maybe_base) |base_member| {
                if (base_member >= vector_id) break;
                out[out_count] = base_member;
                out_count += 1;
                maybe_base = try base_iter.next();
            }
            const present_in_base = if (maybe_base) |base_member| base_member == vector_id else false;
            if (last_op != .tombstone) {
                out[out_count] = vector_id;
                out_count += 1;
            }
            if (present_in_base) maybe_base = try base_iter.next();
        }
        try base_iter.finish();
        return out_count;
    }

    fn canUsePostingOverlayPlan(comptime ScratchType: type) bool {
        const Scratch = switch (@typeInfo(ScratchType)) {
            .pointer => |ptr| ptr.child,
            else => ScratchType,
        };
        return @hasDecl(Scratch, "resetPostingOverlayApply") and
            @hasDecl(Scratch, "ensurePostingOverlayAppendCapacity") and
            @hasField(Scratch, "member_ids");
    }

    fn resetPostingOverlayApply(scratch: anytype) void {
        scratch.resetPostingOverlayApply();
    }

    fn compactMembersWithOverlayPlan(alloc: std.mem.Allocator, scratch: anytype, base_member_count: usize) !usize {
        const appended_count = PostingFormat.overlayAppendedCount(scratch).*;
        try scratch.ensureMemberIdCapacity(alloc, base_member_count + appended_count);
        const base_members = scratch.member_ids[0..base_member_count];
        const out = scratch.member_ids;
        var member_count: usize = 0;
        for (base_members) |member| {
            if (PostingFormat.overlayRemovedMembers(scratch).contains(member)) continue;
            out[member_count] = member;
            member_count += 1;
        }

        const appended_ids = PostingFormat.overlayAppendedIds(scratch);
        const appended_live = PostingFormat.overlayAppendedLive(scratch);
        var append_index: usize = 0;
        while (append_index < appended_count) : (append_index += 1) {
            if (!appended_live[append_index]) continue;
            out[member_count] = appended_ids[append_index];
            member_count += 1;
        }
        return member_count;
    }

    pub fn applyDeltaTailIntoScratch(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
    ) !DeltaReplayResult {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "applyPostingBackendDeltaTailIntoScratch")) {
                return index.applyPostingBackendDeltaTailIntoScratch(txn, posting_id, alloc, scratch, member_count, base_generation);
            }
            return error.UnsupportedPostingBackend;
        }
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var result = DeltaReplayResult{};

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            const entry_result = try PostingFormat.applyDeltaTailAfterGenerationIntoScratch(
                alloc,
                scratch,
                member_count,
                entry.value,
                base_generation,
            );
            result.records += entry_result.records;
            result.max_sequence = @max(result.max_sequence, entry_result.max_sequence);
            maybe_entry = try cursor.next();
        }
        return result;
    }

    pub fn applyDeltaTailIntoScratchAdaptive(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
    ) !DeltaReplayResult {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "applyPostingBackendDeltaTailIntoScratch")) {
                return index.applyPostingBackendDeltaTailIntoScratch(txn, posting_id, alloc, scratch, member_count, base_generation);
            }
            return error.UnsupportedPostingBackend;
        }
        resetPostingOverlayApply(scratch);
        const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(scratch),
        };
        if (comptime @hasDecl(Scratch, "cachedPostingDeltaTail")) {
            if (scratch.cachedPostingDeltaTail(posting_id)) |cached| {
                if (comptime @hasDecl(Scratch, "notePostingDeltaTailCacheHit")) scratch.notePostingDeltaTailCacheHit();
                return try applyCachedDeltaTailIntoScratch(alloc, scratch, member_count, base_generation, cached);
            }
            if (comptime @hasDecl(Scratch, "notePostingDeltaTailCacheMiss")) scratch.notePostingDeltaTailCacheMiss();
        }
        if (comptime @hasDecl(Scratch, "beginPostingDeltaTailCache")) {
            scratch.beginPostingDeltaTailCache(posting_id);
            errdefer scratch.invalidatePostingDeltaTailCache();
        }
        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var result = DeltaReplayResult{};
        var use_overlay_plan = false;
        var buffered: [overlay_plan_min_delta_records]PostingDeltaRecord = undefined;
        var buffered_count: usize = 0;

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                if (comptime @hasDecl(Scratch, "appendPostingDeltaTailCacheRecord")) {
                    try scratch.appendPostingDeltaTailCacheRecord(alloc, record.sequence, record.vector_id, @intFromEnum(record.op));
                }
                if (PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                result.records += 1;
                result.max_sequence = @max(result.max_sequence, record.sequence);
                if (use_overlay_plan) {
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                } else if (buffered_count < buffered.len) {
                    buffered[buffered_count] = record;
                    buffered_count += 1;
                } else {
                    use_overlay_plan = true;
                    var i: usize = 0;
                    while (i < buffered_count) : (i += 1) {
                        try PostingFormat.applyPostingOverlayRecord(alloc, scratch, buffered[i]);
                    }
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                }
            }
            maybe_entry = try cursor.next();
        }
        if (comptime @hasDecl(Scratch, "beginPostingDeltaTailCache")) {
            try prefetchNearbyDeltaTails(alloc, scratch, &cursor, maybe_entry, posting_id);
        }
        if (use_overlay_plan) {
            member_count.* = try compactMembersWithOverlayPlan(alloc, scratch, member_count.*);
        } else {
            try scratch.ensureMemberIdCapacity(alloc, member_count.* + buffered_count);
            for (buffered[0..buffered_count]) |record| {
                PostingFormat.applyDeltaRecordToScratch(scratch, member_count, record);
            }
        }
        return result;
    }

    pub fn applyDeltaTailIntoScratchAdaptiveSorted(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
    ) !DeltaReplayResult {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "applyPostingBackendDeltaTailIntoScratch")) {
                return index.applyPostingBackendDeltaTailIntoScratch(txn, posting_id, alloc, scratch, member_count, base_generation);
            }
            return error.UnsupportedPostingBackend;
        }
        resetPostingOverlayApply(scratch);
        const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(scratch),
        };
        if (comptime @hasDecl(Scratch, "cachedPostingDeltaTail")) {
            if (scratch.cachedPostingDeltaTail(posting_id)) |cached| {
                if (comptime @hasDecl(Scratch, "notePostingDeltaTailCacheHit")) scratch.notePostingDeltaTailCacheHit();
                return try applyCachedDeltaTailIntoSortedScratch(alloc, scratch, member_count, base_generation, cached);
            }
            if (comptime @hasDecl(Scratch, "notePostingDeltaTailCacheMiss")) scratch.notePostingDeltaTailCacheMiss();
        }
        if (comptime @hasDecl(Scratch, "beginPostingDeltaTailCache")) {
            scratch.beginPostingDeltaTailCache(posting_id);
            errdefer scratch.invalidatePostingDeltaTailCache();
        }

        var cursor = try openNamespacedCursor(index, txn, .nodes);
        defer cursor.close();

        var prefix_buf: [10]u8 = undefined;
        const prefix = hbc.encodePostingDeltaPrefix(&prefix_buf, posting_id);
        var result = DeltaReplayResult{};
        var use_overlay_plan = false;
        var compact_ids: [compact_sorted_delta_max_records]VectorId = undefined;
        var compact_ops: [compact_sorted_delta_max_records]PostingDeltaOp = undefined;
        var compact_count: usize = 0;

        var maybe_entry = try cursor.seekAtOrAfter(prefix);
        while (maybe_entry) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) break;
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                if (comptime @hasDecl(Scratch, "appendPostingDeltaTailCacheRecord")) {
                    try scratch.appendPostingDeltaTailCacheRecord(alloc, record.sequence, record.vector_id, @intFromEnum(record.op));
                }
                if (PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                result.records += 1;
                result.max_sequence = @max(result.max_sequence, record.sequence);
                if (use_overlay_plan) {
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                } else if (compact_count < compact_ids.len) {
                    compact_ids[compact_count] = record.vector_id;
                    compact_ops[compact_count] = record.op;
                    compact_count += 1;
                } else {
                    use_overlay_plan = true;
                    try applyCompactOpsToOverlayPlan(alloc, scratch, compact_ids[0..compact_count], compact_ops[0..compact_count]);
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
                }
            }
            maybe_entry = try cursor.next();
        }
        if (comptime @hasDecl(Scratch, "beginPostingDeltaTailCache")) {
            try prefetchNearbyDeltaTails(alloc, scratch, &cursor, maybe_entry, posting_id);
        }
        if (use_overlay_plan) {
            member_count.* = try compactMembersWithOverlayPlan(alloc, scratch, member_count.*);
        } else {
            member_count.* = try applySortedCompactOpsToSortedScratch(alloc, scratch, member_count.*, compact_ids[0..compact_count], compact_ops[0..compact_count]);
        }
        return result;
    }

    fn applyCachedDeltaTailIntoScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
        cached: anytype,
    ) !DeltaReplayResult {
        var result = DeltaReplayResult{};
        var use_overlay_plan = false;
        var buffered: [overlay_plan_min_delta_records]PostingDeltaRecord = undefined;
        var buffered_count: usize = 0;
        var i: usize = 0;
        while (i < cached.ids.len) : (i += 1) {
            if (PostingFormat.deltaSequenceGeneration(cached.sequences[i]) <= base_generation) continue;
            const record = PostingDeltaRecord{
                .sequence = cached.sequences[i],
                .op = try cachedPostingDeltaOp(cached.ops[i]),
                .vector_id = cached.ids[i],
            };
            result.records += 1;
            result.max_sequence = @max(result.max_sequence, record.sequence);
            if (use_overlay_plan) {
                try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
            } else if (buffered_count < buffered.len) {
                buffered[buffered_count] = record;
                buffered_count += 1;
            } else {
                use_overlay_plan = true;
                var buffered_index: usize = 0;
                while (buffered_index < buffered_count) : (buffered_index += 1) {
                    try PostingFormat.applyPostingOverlayRecord(alloc, scratch, buffered[buffered_index]);
                }
                try PostingFormat.applyPostingOverlayRecord(alloc, scratch, record);
            }
        }
        if (use_overlay_plan) {
            member_count.* = try compactMembersWithOverlayPlan(alloc, scratch, member_count.*);
        } else {
            try scratch.ensureMemberIdCapacity(alloc, member_count.* + buffered_count);
            for (buffered[0..buffered_count]) |record| {
                PostingFormat.applyDeltaRecordToScratch(scratch, member_count, record);
            }
        }
        return result;
    }

    fn applyCachedDeltaTailIntoSortedScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
        cached: anytype,
    ) !DeltaReplayResult {
        var compact_ids: [compact_sorted_delta_max_records]VectorId = undefined;
        var compact_ops: [compact_sorted_delta_max_records]PostingDeltaOp = undefined;
        var compact_count: usize = 0;
        var result = DeltaReplayResult{};
        var i: usize = 0;
        while (i < cached.ids.len) : (i += 1) {
            if (PostingFormat.deltaSequenceGeneration(cached.sequences[i]) <= base_generation) continue;
            result.records += 1;
            result.max_sequence = @max(result.max_sequence, cached.sequences[i]);
            if (compact_count >= compact_ids.len) {
                return try applyCachedDeltaTailIntoScratch(alloc, scratch, member_count, base_generation, cached);
            }
            compact_ids[compact_count] = cached.ids[i];
            compact_ops[compact_count] = try cachedPostingDeltaOp(cached.ops[i]);
            compact_count += 1;
        }
        member_count.* = try applySortedCompactOpsToSortedScratch(alloc, scratch, member_count.*, compact_ids[0..compact_count], compact_ops[0..compact_count]);
        return result;
    }

    fn applyCompactOpsToOverlayPlan(alloc: std.mem.Allocator, scratch: anytype, ids: []const VectorId, ops: []const PostingDeltaOp) !void {
        for (ids, ops) |vector_id, op| {
            try PostingFormat.applyPostingOverlayRecord(alloc, scratch, .{
                .sequence = 0,
                .op = op,
                .vector_id = vector_id,
            });
        }
    }

    fn applySortedCompactOpsToSortedScratch(
        alloc: std.mem.Allocator,
        scratch: anytype,
        base_member_count: usize,
        ids: []VectorId,
        ops: []PostingDeltaOp,
    ) !usize {
        stableSortCompactOpsByVector(ids, ops);
        try scratch.ensurePostingOverlayAppendCapacity(alloc, base_member_count + ids.len);
        const base_members = scratch.member_ids[0..base_member_count];
        const out = PostingFormat.overlayAppendedIds(scratch);
        var out_count: usize = 0;
        var base_index: usize = 0;
        var op_index: usize = 0;
        while (base_index < base_members.len or op_index < ids.len) {
            if (op_index >= ids.len) {
                out[out_count] = base_members[base_index];
                out_count += 1;
                base_index += 1;
                continue;
            }
            const vector_id = ids[op_index];
            var last_op = ops[op_index];
            op_index += 1;
            while (op_index < ids.len and ids[op_index] == vector_id) : (op_index += 1) {
                last_op = ops[op_index];
            }
            while (base_index < base_members.len and base_members[base_index] < vector_id) : (base_index += 1) {
                out[out_count] = base_members[base_index];
                out_count += 1;
            }
            const present_in_base = base_index < base_members.len and base_members[base_index] == vector_id;
            if (last_op != .tombstone) {
                out[out_count] = vector_id;
                out_count += 1;
            }
            if (present_in_base) base_index += 1;
        }
        try scratch.ensureMemberIdCapacity(alloc, out_count);
        @memcpy(scratch.member_ids[0..out_count], out[0..out_count]);
        return out_count;
    }

    fn stableSortCompactOpsByVector(ids: []VectorId, ops: []PostingDeltaOp) void {
        var i: usize = 1;
        while (i < ids.len) : (i += 1) {
            const id = ids[i];
            const op = ops[i];
            var j = i;
            while (j > 0 and ids[j - 1] > id) : (j -= 1) {
                ids[j] = ids[j - 1];
                ops[j] = ops[j - 1];
            }
            ids[j] = id;
            ops[j] = op;
        }
    }

    fn prefetchNearbyDeltaTails(
        alloc: std.mem.Allocator,
        scratch: anytype,
        cursor: *vector_store.Cursor,
        first_entry: ?vector_store.Entry,
        current_posting_id: PostingId,
    ) !void {
        var maybe_entry = first_entry;
        var cached_postings: usize = 0;
        var active_posting_id: ?PostingId = null;
        const prefetch_limit = if (comptime @hasDecl(std.meta.Child(@TypeOf(scratch)), "postingDeltaTailPrefetchLimit"))
            scratch.postingDeltaTailPrefetchLimit()
        else
            delta_tail_prefetch_posting_count;
        if (prefetch_limit == 0) return;
        while (maybe_entry) |entry| {
            const next_posting_id = postingDeltaKeyPostingId(entry.key) orelse break;
            if (next_posting_id <= current_posting_id) break;
            if (active_posting_id == null or active_posting_id.? != next_posting_id) {
                if (cached_postings >= prefetch_limit) break;
                scratch.beginPostingDeltaTailCache(next_posting_id);
                errdefer scratch.invalidatePostingDeltaTailCache();
                active_posting_id = next_posting_id;
                cached_postings += 1;
            }
            if (comptime @hasDecl(std.meta.Child(@TypeOf(scratch)), "notePostingDeltaTailPrefetchDecodedBytes")) {
                scratch.notePostingDeltaTailPrefetchDecodedBytes(entry.value.len);
            }
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                try scratch.appendPostingDeltaTailCacheRecord(alloc, record.sequence, record.vector_id, @intFromEnum(record.op));
            }
            maybe_entry = try cursor.next();
        }
    }

    fn cachedPostingDeltaOp(raw: u8) !PostingDeltaOp {
        return switch (raw) {
            @intFromEnum(PostingDeltaOp.insert) => .insert,
            @intFromEnum(PostingDeltaOp.tombstone) => .tombstone,
            @intFromEnum(PostingDeltaOp.replace) => .replace,
            else => error.Corrupted,
        };
    }

    fn postingDeltaKeyPostingId(key: []const u8) ?PostingId {
        if (key.len != 18 or key[0] != 'P' or key[1] != 'D') return null;
        return std.mem.readInt(u64, key[2..10], .big);
    }

    pub fn materializeBaseDeltaMembers(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]VectorId {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "loadPostingBackendBaseData") and @hasDecl(Index, "applyPostingBackendDeltaTailIntoScratch")) {
                const base_data = try index.loadPostingBackendBaseData(txn, posting_id, is_not_found);
                defer index.alloc.free(base_data);
                var scratch = FoldScratch{};
                defer scratch.deinit(index.alloc);
                const base_header = try PostingFormat.decodeBaseIntoScratch(index.alloc, &scratch, base_data);
                var materialized_len = base_header.member_count;
                if (baseDeltaIsCanonical(index))
                    _ = try applyDeltaTailIntoScratchAdaptiveSorted(index, txn, posting_id, index.alloc, &scratch, &materialized_len, base_header.generation)
                else
                    _ = try applyDeltaTailIntoScratchAdaptive(index, txn, posting_id, index.alloc, &scratch, &materialized_len, base_header.generation);
                return try index.alloc.dupe(VectorId, scratch.member_ids[0..materialized_len]);
            }
            if (comptime @hasDecl(Index, "materializePostingBackendMembers")) {
                return index.materializePostingBackendMembers(txn, posting_id, is_not_found);
            }
            return error.UnsupportedPostingBackend;
        }
        const base_data = try loadBaseData(index, txn, posting_id, is_not_found);
        var scratch = FoldScratch{};
        defer scratch.deinit(index.alloc);
        const base_header = try PostingFormat.decodeBaseIntoScratch(index.alloc, &scratch, base_data);
        var materialized_len = base_header.member_count;
        _ = try applyDeltaTailIntoScratchAdaptive(index, txn, posting_id, index.alloc, &scratch, &materialized_len, base_header.generation);
        return try index.alloc.dupe(VectorId, scratch.member_ids[0..materialized_len]);
    }

    fn materializedMemberCapacityEstimate(base_member_count: usize, stats: PostingDeltaTailStats) usize {
        return base_member_count +| stats.records_after_generation;
    }

    fn deltaTailShouldFold(base_member_count: usize, stats: PostingDeltaTailStats, options: FoldDeltaTailOptions) bool {
        if (stats.records == 0) return true;
        if (stats.records_after_generation == 0) return true;
        if (stats.records_after_generation >= options.min_delta_records) return true;

        if (options.min_tombstone_records != 0 and stats.tombstones_after_generation >= options.min_tombstone_records) return true;
        if (options.min_delta_value_bytes != 0 and stats.encoded_value_bytes >= options.min_delta_value_bytes) return true;
        if (options.min_delta_to_base_ratio_bps != 0) {
            const denominator = @max(base_member_count, @as(usize, 1));
            const ratio_bps = (stats.records_after_generation * 10_000) / denominator;
            if (ratio_bps >= options.min_delta_to_base_ratio_bps) return true;
        }
        return false;
    }

    fn deltaTailExceedsMaterializedLimit(base_member_count: usize, stats: PostingDeltaTailStats, options: FoldDeltaTailOptions) bool {
        const estimated_members = materializedMemberCapacityEstimate(base_member_count, stats);
        if (estimated_members > options.max_materialized_members) return true;
        const estimated_bytes = std.math.mul(usize, estimated_members, @sizeOf(VectorId)) catch return true;
        return estimated_bytes > options.max_materialized_bytes;
    }

    fn saveEncodedBase(index: anytype, txn: anytype, posting_id: PostingId, encoded: []const u8) !void {
        var key_buf: [10]u8 = undefined;
        const key = hbc.encodePostingBaseKey(&key_buf, posting_id);
        try index.putNamespaced(txn, .nodes, key, encoded);
        notePostingBasePut(index, key.len, encoded);
    }

    pub fn foldDeltaTailIntoBaseWithOptions(
        index: anytype,
        txn: anytype,
        posting_id: PostingId,
        is_not_found: fn (anyerror) bool,
        options: FoldDeltaTailOptions,
    ) !FoldDeltaTailResult {
        if (useSegmentPostingBackend(index)) {
            const Index = IndexType(@TypeOf(index));
            if (comptime @hasDecl(Index, "foldPostingBackendDeltaTailIntoBase")) {
                return index.foldPostingBackendDeltaTailIntoBase(txn, posting_id, is_not_found, options);
            }
            return error.UnsupportedPostingBackend;
        }
        const base_data = try loadBaseData(index, txn, posting_id, is_not_found);
        const base_header = try PostingFormat.decodeBaseHeader(base_data);
        var scratch = if (comptime @hasDecl(@TypeOf(index.*), "acquirePostingFoldScratch"))
            try index.acquirePostingFoldScratch()
        else
            FoldScratch{};
        defer {
            if (comptime @hasDecl(@TypeOf(index.*), "releasePostingFoldScratch")) {
                index.releasePostingFoldScratch(&scratch);
            } else {
                scratch.deinit(index.alloc);
            }
        }
        const sort_base_members = shouldSortBaseMembers(index);
        const configured_base_member_block_size = postingBaseMemberBlockSize(index);
        const adaptive_scan = try scanDeltaTailAdaptiveForFold(index, txn, posting_id, index.alloc, &scratch, base_header.generation, sort_base_members);
        var peak_scratch_bytes = scratch.bytes();
        const use_overlay_plan = adaptive_scan.use_overlay_plan;
        const stats = adaptive_scan.stats;
        if (stats.records == 0) {
            return .{
                .base_member_count = base_header.member_count,
                .materialized_member_count = base_header.member_count,
                .peak_scratch_bytes = @intCast(peak_scratch_bytes),
                .next_generation = base_header.generation,
            };
        }
        if (stats.records_after_generation == 0) {
            const deleted_tail = try deleteDeltaTail(index, txn, posting_id);
            notePostingDeltaFold(index, 0, base_header.member_count, base_header.member_count, deleted_tail, 0, 0, peak_scratch_bytes);
            return .{
                .delta_records = stats.records,
                .base_member_count = base_header.member_count,
                .materialized_member_count = base_header.member_count,
                .deleted_tail_keys = deleted_tail.keys,
                .deleted_tail_key_bytes = deleted_tail.key_bytes,
                .deleted_tail_value_bytes = deleted_tail.value_bytes,
                .peak_scratch_bytes = @intCast(peak_scratch_bytes),
                .next_generation = base_header.generation,
            };
        }
        if (!deltaTailShouldFold(base_header.member_count, stats, options)) {
            return .{
                .delta_records = stats.records,
                .base_member_count = base_header.member_count,
                .materialized_member_count = base_header.member_count,
                .peak_scratch_bytes = @intCast(peak_scratch_bytes),
                .next_generation = base_header.generation,
                .skipped = true,
            };
        }
        if (deltaTailExceedsMaterializedLimit(base_header.member_count, stats, options)) {
            return .{
                .delta_records = stats.records,
                .base_member_count = base_header.member_count,
                .materialized_member_count = base_header.member_count,
                .peak_scratch_bytes = @intCast(peak_scratch_bytes),
                .next_generation = base_header.generation,
                .skipped = true,
            };
        }

        const next_generation = base_header.generation +| 1;
        const folded_base: PostingFormat.EncodedBaseResult = if (use_overlay_plan) blk: {
            const sorted_appended = if (sort_base_members)
                try PostingFormat.collectSortedLiveAppended(index.alloc, &scratch)
            else
                &.{};
            const base_member_block_size = if (sort_base_members)
                try postingBaseMemberBlockSizeForSortedOverlay(index, base_data, &scratch, sorted_appended)
            else
                PostingBaseBlockSizeChoice{
                    .block_size = configured_base_member_block_size,
                    .encoded_len = (try PostingFormat.encodedBaseSizeWithOverlayPlanWithBlockSize(&scratch, base_data, configured_base_member_block_size)).encoded_len,
                };
            const exact_size = if (sort_base_members)
                base_member_block_size.encoded_len
            else
                base_member_block_size.encoded_len;
            try scratch.ensureEncodedBaseCapacity(index.alloc, exact_size);
            break :blk if (sort_base_members)
                try PostingFormat.encodeSortedBaseWithPreparedAppended(&scratch, base_data, sorted_appended, posting_id, next_generation, base_member_block_size.block_size)
            else
                try PostingFormat.encodeBaseWithOverlayPlanWithBlockSize(&scratch, base_data, posting_id, next_generation, base_member_block_size.block_size);
        } else blk: {
            if (sort_base_members) {
                stableSortCompactDeltaRecordsByVector(&scratch);
                const base_member_block_size = try postingBaseMemberBlockSizeForCompactDeltaRecords(index, base_data, &scratch);
                try scratch.ensureEncodedBaseCapacity(index.alloc, base_member_block_size.encoded_len);
                break :blk try PostingFormat.encodeSortedBaseWithCompactDeltaRecords(&scratch, base_data, posting_id, next_generation, base_member_block_size.block_size);
            }
            const materialized_len = materialized: {
                _ = try PostingFormat.decodeBaseIntoScratch(index.alloc, &scratch, base_data);
                var materialized_len = base_header.member_count;
                try scratch.ensureMemberIdCapacity(index.alloc, materialized_len + scratch.deltaRecordCount());
                for (scratch.deltaRecords()) |record| {
                    PostingFormat.applyDeltaRecordToScratch(&scratch, &materialized_len, record);
                }
                break :materialized materialized_len;
            };
            const base_member_block_size = try postingBaseMemberBlockSizeForMembers(index, scratch.member_ids[0..materialized_len]);
            try scratch.ensureEncodedBaseCapacity(index.alloc, base_member_block_size.encoded_len);
            break :blk .{
                .encoded = try PostingFormat.encodeBaseMembersKnownSizeIntoWithBlockSize(scratch.encoded_base, posting_id, next_generation, scratch.member_ids[0..materialized_len], base_member_block_size.block_size),
                .encoded_len = base_member_block_size.encoded_len,
                .member_count = materialized_len,
            };
        };
        peak_scratch_bytes = @max(peak_scratch_bytes, scratch.bytes());
        const materialized_len = folded_base.member_count;
        const encoded = folded_base.encoded;
        var base_key_buf: [10]u8 = undefined;
        const written_base_key_bytes = hbc.encodePostingBaseKey(&base_key_buf, posting_id).len;
        const written_base_value_bytes = encoded.len;
        try saveEncodedBase(index, txn, posting_id, encoded);
        const deleted_tail = try deleteDeltaTail(index, txn, posting_id);
        notePostingDeltaFold(index, stats.records_after_generation, base_header.member_count, materialized_len, deleted_tail, written_base_key_bytes, written_base_value_bytes, peak_scratch_bytes);
        return .{
            .delta_records = stats.records,
            .base_member_count = base_header.member_count,
            .materialized_member_count = materialized_len,
            .deleted_tail_keys = deleted_tail.keys,
            .deleted_tail_key_bytes = deleted_tail.key_bytes,
            .deleted_tail_value_bytes = deleted_tail.value_bytes,
            .written_base_key_bytes = written_base_key_bytes,
            .written_base_value_bytes = written_base_value_bytes,
            .peak_scratch_bytes = @intCast(peak_scratch_bytes),
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

fn shouldHashRemovedMembers(member_count: usize, removed_count: usize) bool {
    return member_count >= 32 and removed_count >= 8;
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

fn notePostingBasePut(index: anytype, key_len: usize, encoded: []const u8) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "write_profile")) return;
    const stats = PostingFormat.decodeBaseStats(encoded) catch return;
    index.write_profile.posting_base_put_calls += 1;
    index.write_profile.posting_base_key_bytes += @intCast(key_len);
    index.write_profile.posting_base_value_bytes += @intCast(stats.encoded_len);
    if (comptime @hasField(@TypeOf(index.write_profile), "posting_base_members")) {
        index.write_profile.posting_base_members += @intCast(stats.header.member_count);
    }
    if (comptime @hasField(@TypeOf(index.write_profile), "posting_base_blocks")) {
        index.write_profile.posting_base_blocks += @intCast(stats.block_count);
    }
    if (comptime @hasField(@TypeOf(index.write_profile), "posting_base_fixed_width_value_bytes")) {
        index.write_profile.posting_base_fixed_width_value_bytes += @intCast(PostingFormat.base_header_size + stats.header.member_count * @sizeOf(VectorId));
    }
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

fn notePostingDeltaFold(index: anytype, record_count: usize, base_member_count: usize, materialized_member_count: usize, deleted_tail: DeleteDeltaTailStats, written_base_key_bytes: usize, written_base_value_bytes: usize, peak_scratch_bytes: u64) void {
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
    if (comptime @hasField(@TypeOf(index.write_profile), "posting_delta_fold_peak_scratch_bytes")) {
        index.write_profile.posting_delta_fold_peak_scratch_bytes = @max(index.write_profile.posting_delta_fold_peak_scratch_bytes, peak_scratch_bytes);
    }
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

fn shouldSortBaseMembers(index: anytype) bool {
    return baseDeltaIsCanonical(index);
}

const PostingBaseBlockSizeChoice = struct {
    block_size: usize,
    encoded_len: usize,
};

fn postingBaseMemberBlockSize(index: anytype) usize {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime !@hasField(Index, "config")) return PostingFormat.base_member_default_block_size;
    if (comptime !@hasField(@TypeOf(index.config), "posting_base_member_block_size")) return PostingFormat.base_member_default_block_size;
    return PostingFormat.normalizeBaseMemberBlockSize(index.config.posting_base_member_block_size);
}

fn postingBaseMemberBlockSizeForMembers(index: anytype, members: []const VectorId) !PostingBaseBlockSizeChoice {
    const configured = postingBaseMemberBlockSize(index);
    if (!shouldSortBaseMembers(index) or configured != PostingFormat.base_member_default_block_size) {
        return .{
            .block_size = configured,
            .encoded_len = try PostingFormat.encodedBaseSizeForMembersWithBlockSize(members, configured),
        };
    }
    return try autoPostingBaseMemberBlockSizeForMembers(members);
}

fn postingBaseMemberBlockSizeForSortedOverlay(index: anytype, base_data: []const u8, scratch: anytype, sorted_appended: []const VectorId) !PostingBaseBlockSizeChoice {
    const configured = postingBaseMemberBlockSize(index);
    if (!shouldSortBaseMembers(index) or configured != PostingFormat.base_member_default_block_size) {
        const size = try PostingFormat.encodedSortedBaseSizeWithPreparedAppended(base_data, scratch, sorted_appended, configured);
        return .{ .block_size = configured, .encoded_len = size.encoded_len };
    }
    return selectAutoPostingBaseMemberBlockSize(.{
        .size16 = (try PostingFormat.encodedSortedBaseSizeWithPreparedAppended(base_data, scratch, sorted_appended, 16)).encoded_len,
        .size32 = (try PostingFormat.encodedSortedBaseSizeWithPreparedAppended(base_data, scratch, sorted_appended, 32)).encoded_len,
        .size64 = (try PostingFormat.encodedSortedBaseSizeWithPreparedAppended(base_data, scratch, sorted_appended, 64)).encoded_len,
    });
}

fn postingBaseMemberBlockSizeForCompactDeltaRecords(index: anytype, base_data: []const u8, scratch: anytype) !PostingBaseBlockSizeChoice {
    const configured = postingBaseMemberBlockSize(index);
    if (!shouldSortBaseMembers(index) or configured != PostingFormat.base_member_default_block_size) {
        const size = try PostingFormat.encodedSortedBaseSizeWithCompactDeltaRecords(base_data, scratch, configured);
        return .{ .block_size = configured, .encoded_len = size.encoded_len };
    }
    return selectAutoPostingBaseMemberBlockSize(.{
        .size16 = (try PostingFormat.encodedSortedBaseSizeWithCompactDeltaRecords(base_data, scratch, 16)).encoded_len,
        .size32 = (try PostingFormat.encodedSortedBaseSizeWithCompactDeltaRecords(base_data, scratch, 32)).encoded_len,
        .size64 = (try PostingFormat.encodedSortedBaseSizeWithCompactDeltaRecords(base_data, scratch, 64)).encoded_len,
    });
}

fn autoPostingBaseMemberBlockSizeForMembers(members: []const VectorId) !PostingBaseBlockSizeChoice {
    return selectAutoPostingBaseMemberBlockSize(.{
        .size16 = try PostingFormat.encodedBaseSizeForMembersWithBlockSize(members, 16),
        .size32 = try PostingFormat.encodedBaseSizeForMembersWithBlockSize(members, 32),
        .size64 = try PostingFormat.encodedBaseSizeForMembersWithBlockSize(members, 64),
    });
}

const PostingBaseBlockSizeCandidates = struct {
    size16: usize,
    size32: usize,
    size64: usize,
};

fn selectAutoPostingBaseMemberBlockSize(candidates: PostingBaseBlockSizeCandidates) PostingBaseBlockSizeChoice {
    var best_size = candidates.size32;
    var best_block: usize = 32;
    if (candidates.size64 <= best_size) {
        best_size = candidates.size64;
        best_block = 64;
    }
    if (candidates.size16 + (candidates.size16 / 32) < best_size) {
        best_size = candidates.size16;
        best_block = 16;
    }
    return .{ .block_size = best_block, .encoded_len = best_size };
}

fn sortVectorIdsAsc(ids: []VectorId) void {
    std.mem.sort(VectorId, ids, {}, comptime std.sort.asc(VectorId));
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

fn notePostingBaseDecode(profile: anytype, elapsed_ns: u64, member_count: usize) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_base_decode_ns")) return;
    profile.posting_base_decode_ns += elapsed_ns;
    profile.posting_base_decode_members += @intCast(member_count);
}

fn notePostingDeltaReplay(profile: anytype, elapsed_ns: u64, record_count: usize) void {
    const Profile = switch (@typeInfo(@TypeOf(profile))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(profile),
    };
    if (comptime !@hasField(Profile, "posting_delta_replay_ns")) return;
    profile.posting_delta_replay_ns += elapsed_ns;
    profile.posting_delta_replay_records += @intCast(record_count);
}

fn copyCachedPostingMembersIfAvailable(
    scratch: anytype,
    posting_view: PostingView,
    base_generation: u64,
    max_delta_sequence: u64,
    profile: anytype,
) ?[]const VectorId {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (comptime !@hasDecl(Scratch, "cachedPostingMembers")) return null;
    const cached = scratch.cachedPostingMembers(posting_view.id, base_generation, posting_view.state.mutation_version, max_delta_sequence) orelse return null;
    notePostingOverlayCacheHit(profile);
    return cached;
}

fn cachePostingMembersIfAvailable(
    scratch: anytype,
    alloc: std.mem.Allocator,
    posting_view: PostingView,
    base_generation: u64,
    max_delta_sequence: u64,
    members: []const VectorId,
    profile: anytype,
) !void {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (comptime !@hasDecl(Scratch, "cachePostingMembers")) return;
    const result = try scratch.cachePostingMembers(alloc, posting_view.id, base_generation, posting_view.state.mutation_version, max_delta_sequence, members);
    notePostingOverlayCacheResult(profile, result.evictions, result.admission_skips, result.member_bytes);
}

fn decodeBaseHeaderCachedIfAvailable(scratch: anytype, posting_id: PostingId, base_data: []const u8) !PostingBaseHeader {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (base_data.len < PostingFormat.encoded_base_header_size) return try PostingFormat.decodeBaseHeader(base_data);
    const header_bytes = base_data[0..PostingFormat.encoded_base_header_size];
    if (comptime @hasDecl(Scratch, "cachedPostingBaseHeader")) {
        if (scratch.cachedPostingBaseHeader(posting_id, header_bytes)) |cached| {
            return .{
                .posting_id = posting_id,
                .generation = cached.generation,
                .member_count = cached.member_count,
            };
        }
    }
    const header = try PostingFormat.decodeBaseHeader(base_data);
    if (comptime @hasDecl(Scratch, "cachePostingBaseHeader")) {
        scratch.cachePostingBaseHeader(posting_id, header_bytes, header.generation, header.member_count);
    }
    return header;
}

fn notePostingMemberCacheMissIfAvailable(
    scratch: anytype,
    alloc: std.mem.Allocator,
    posting_id: PostingId,
) !void {
    const Scratch = switch (@typeInfo(@TypeOf(scratch))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(scratch),
    };
    if (comptime !@hasDecl(Scratch, "notePostingMemberCacheMiss")) return;
    try scratch.notePostingMemberCacheMiss(alloc, posting_id);
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

fn byteLen(values: anytype) u64 {
    return @as(u64, @intCast(values.len * @sizeOf(std.meta.Child(@TypeOf(values)))));
}

fn approximateHashMapBytes(capacity: usize, comptime key_size: usize, comptime value_size: usize) u64 {
    if (capacity == 0) return 0;
    return @intCast(capacity * (key_size + value_size + 2));
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

test "posting store hashes large member removals while preserving order" {
    const alloc = std.testing.allocator;
    const members = try alloc.alloc(u64, 40);
    for (members, 0..) |*member, i| member.* = @intCast(i + 1);
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

    const removed = try PostingStore.removeMembers(alloc, &node, &[_]u64{ 2, 4, 6, 8, 10, 12, 14, 16, 16, 999 });
    try std.testing.expectEqual(@as(usize, 8), removed);
    try std.testing.expectEqual(@as(usize, 32), node.members.len);
    try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 3, 5, 7, 9 }, node.members[0..5]);
    try std.testing.expectEqualSlices(u64, &[_]u64{ 34, 35, 36, 37, 38, 39, 40 }, node.members[node.members.len - 7 ..]);

    const no_match = try PostingStore.removeMembers(alloc, &node, &[_]u64{ 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007 });
    try std.testing.expectEqual(@as(usize, 0), no_match);
    try std.testing.expectEqual(@as(usize, 32), node.members.len);
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

    try std.testing.expectEqual(try PostingFormat.encodedBaseSize(.{
        .posting_id = 7,
        .generation = 11,
        .members = members[0..],
    }), encoded.len);

    var decoded = try PostingFormat.decodeBase(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(PostingId, 7), decoded.posting_id);
    try std.testing.expectEqual(@as(u64, 11), decoded.generation);
    try std.testing.expectEqualSlices(VectorId, members[0..], decoded.members);
    encoded[4] = 2;
    try std.testing.expectError(error.UnsupportedPostingBaseVersion, PostingFormat.decodeBase(alloc, encoded));
}

test "posting base stats validates encoded blocks without materializing members" {
    const alloc = std.testing.allocator;
    var members: [33]VectorId = undefined;
    for (&members, 0..) |*member, i| member.* = @as(VectorId, @intCast(i + 1)) * 10;

    const encoded = try PostingFormat.encodeBaseWithBlockSize(alloc, .{
        .posting_id = 7,
        .generation = 11,
        .members = members[0..],
    }, 16);
    defer alloc.free(encoded);

    const stats = try PostingFormat.decodeBaseStats(encoded);
    try std.testing.expectEqual(@as(PostingId, 7), stats.header.posting_id);
    try std.testing.expectEqual(@as(u64, 11), stats.header.generation);
    try std.testing.expectEqual(members.len, stats.header.member_count);
    try std.testing.expectEqual(@as(usize, 3), stats.block_count);
    try std.testing.expectEqual(encoded.len, stats.encoded_len);

    const header = try PostingFormat.decodeBaseHeader(encoded[0 .. encoded.len - 1]);
    try std.testing.expectEqual(members.len, header.member_count);
    try std.testing.expectError(error.Corrupted, PostingFormat.decodeBaseStats(encoded[0 .. encoded.len - 1]));
    try std.testing.expectError(error.Corrupted, PostingFormat.validateBase(encoded[0 .. encoded.len - 1]));
}

test "posting base sorted membership streams without full materialization" {
    const alloc = std.testing.allocator;
    var members: [33]VectorId = undefined;
    for (&members, 0..) |*member, i| member.* = @as(VectorId, @intCast(i + 1)) * 10;

    const encoded = try PostingFormat.encodeBaseWithBlockSize(alloc, .{
        .posting_id = 7,
        .generation = 11,
        .members = members[0..],
    }, 16);
    defer alloc.free(encoded);

    try std.testing.expect(try PostingFormat.baseContainsSortedMember(encoded, 10));
    try std.testing.expect(try PostingFormat.baseContainsSortedMember(encoded, 170));
    try std.testing.expect(!try PostingFormat.baseContainsSortedMember(encoded, 99));
    try std.testing.expect(!try PostingFormat.baseContainsSortedMember(encoded, 999));

    const corrupt = try alloc.alloc(u8, encoded.len + 1);
    defer alloc.free(corrupt);
    @memcpy(corrupt[0..encoded.len], encoded);
    corrupt[encoded.len] = 0;

    try std.testing.expect(try PostingFormat.baseContainsSortedMember(corrupt, 10));
    try std.testing.expect(!try PostingFormat.baseContainsSortedMember(corrupt, 15));
    try std.testing.expectError(error.Corrupted, PostingFormat.baseContainsSortedMemberStrict(corrupt, 10));
    try std.testing.expectError(error.Corrupted, PostingFormat.baseContainsSortedMemberStrict(corrupt, 15));

    var pos: usize = PostingFormat.encoded_base_header_size;
    var remaining_members = members.len;
    const first_block_count = try PostingFormat.readBaseBlockCount(encoded, &pos, remaining_members);
    _ = try PostingFormat.readVarint(encoded, &pos);
    var first_block_index: usize = 0;
    while (first_block_index < first_block_count) : (first_block_index += 1) {
        _ = try PostingFormat.readVarint(encoded, &pos);
    }
    remaining_members -= first_block_count;
    _ = try PostingFormat.readBaseBlockCount(encoded, &pos, remaining_members);
    _ = try PostingFormat.readVarint(encoded, &pos);
    const truncated_after_second_block_min = encoded[0..pos];

    try std.testing.expect(!try PostingFormat.baseContainsSortedMember(truncated_after_second_block_min, 165));
    try std.testing.expectError(error.Corrupted, PostingFormat.baseContainsSortedMemberStrict(truncated_after_second_block_min, 165));
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

    try std.testing.expectEqual(PostingFormat.version, encoded[4]);
    try std.testing.expectEqual(@as(usize, 17 + records.len * 3), encoded.len);

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

test "posting delta tail uses varint encoding for small single-record values" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 7, .op = .insert, .vector_id = 10 },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);

    try std.testing.expectEqual(PostingFormat.version, encoded[4] & 0x7f);
    try std.testing.expect((encoded[4] & 0x80) != 0);
    try std.testing.expect(encoded.len < PostingFormat.delta_header_size);

    const decoded = try PostingFormat.decodeDeltaTail(alloc, encoded);
    defer alloc.free(decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(records[0].sequence, decoded[0].sequence);
    try std.testing.expectEqual(records[0].op, decoded[0].op);
    try std.testing.expectEqual(records[0].vector_id, decoded[0].vector_id);
}

test "posting delta tail rejects sequence offset overflow" {
    const alloc = std.testing.allocator;
    var encoded = try alloc.alloc(u8, 20);
    defer alloc.free(encoded);
    @memcpy(encoded[0..4], &PostingFormat.delta_magic);
    encoded[4] = PostingFormat.version;
    std.mem.writeInt(u32, encoded[5..9], 1, .little);
    std.mem.writeInt(u64, encoded[9..17], std.math.maxInt(u64), .little);
    encoded[17] = 1;
    encoded[18] = @intFromEnum(PostingDeltaOp.insert);
    encoded[19] = 10;

    try std.testing.expectError(error.Corrupted, PostingFormat.decodeDeltaTail(alloc, encoded));
}

test "posting delta tail rejects unsupported op" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 1, .op = .insert, .vector_id = std.math.maxInt(u64) },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);

    try std.testing.expectEqual(PostingFormat.version, encoded[4] & 0x7f);
    var pos: usize = 5;
    _ = try PostingFormat.readVarint(encoded, &pos);
    encoded[pos] = 99;
    try std.testing.expectError(error.UnsupportedPostingDeltaOp, PostingFormat.decodeDeltaTail(alloc, encoded));
}

test "posting delta tail rejects truncated varint encoding" {
    const alloc = std.testing.allocator;
    const records = [_]PostingDeltaRecord{
        .{ .sequence = 1, .op = .insert, .vector_id = 10 },
    };

    const encoded = try PostingFormat.encodeDeltaTail(alloc, records[0..]);
    defer alloc.free(encoded);
    try std.testing.expectEqual(PostingFormat.version, encoded[4] & 0x7f);
    try std.testing.expectError(error.Corrupted, PostingFormat.decodeDeltaTail(alloc, encoded[0 .. encoded.len - 1]));
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
    posting_backend_base_saves: u64 = 0,
    posting_backend_centroid_saves: u64 = 0,
    posting_backend_delta_appends: u64 = 0,
    posting_backend_base_loads: u64 = 0,
    posting_backend_member_materializations: u64 = 0,

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

    pub fn savePostingBackendBase(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, encoded: []const u8) !void {
        self.posting_backend_base_saves += 1;
        var key_buf: [10]u8 = undefined;
        try self.putNamespaced(txn, .nodes, hbc.encodePostingBaseKey(&key_buf, posting_id), encoded);
    }

    pub fn savePostingBackendCentroidDirectory(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, encoded: []const u8) !void {
        self.posting_backend_centroid_saves += 1;
        var key_buf: [10]u8 = undefined;
        try self.putNamespaced(txn, .nodes, hbc.encodeCentroidDirectoryKey(&key_buf, posting_id), encoded);
    }

    pub fn savePostingBackendBaseAndCentroidDirectory(
        self: *PostingPersistenceTestIndex,
        txn: anytype,
        base_posting_id: PostingId,
        encoded_base: []const u8,
        centroid_posting_id: PostingId,
        encoded_centroid: []const u8,
    ) !void {
        try self.savePostingBackendBase(txn, base_posting_id, encoded_base);
        try self.savePostingBackendCentroidDirectory(txn, centroid_posting_id, encoded_centroid);
    }

    pub fn appendPostingBackendDeltaRecords(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, records: []const PostingDeltaRecord) !void {
        self.posting_backend_delta_appends += 1;
        const encoded = try PostingFormat.encodeDeltaTail(self.alloc, records);
        defer self.alloc.free(encoded);
        var key_buf: [18]u8 = undefined;
        try self.appendNamespaced(txn, .nodes, hbc.encodePostingDeltaKey(&key_buf, posting_id, records[0].sequence), encoded);
    }

    fn postingBackendBaseData(self: *PostingPersistenceTestIndex, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]const u8 {
        var key_buf: [10]u8 = undefined;
        return self.getNamespaced(.{}, .nodes, hbc.encodePostingBaseKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
    }

    pub fn loadPostingBackendBase(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedPostingBase {
        _ = txn;
        self.posting_backend_base_loads += 1;
        return try PostingFormat.decodeBase(self.alloc, try self.postingBackendBaseData(posting_id, is_not_found));
    }

    pub fn loadPostingBackendBaseHeader(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingBaseHeader {
        _ = txn;
        self.posting_backend_base_loads += 1;
        return try PostingFormat.decodeBaseHeader(try self.postingBackendBaseData(posting_id, is_not_found));
    }

    pub fn loadPostingBackendBaseStats(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingBaseStats {
        _ = txn;
        self.posting_backend_base_loads += 1;
        return try PostingFormat.decodeBaseStats(try self.postingBackendBaseData(posting_id, is_not_found));
    }

    pub fn loadPostingBackendBaseData(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]u8 {
        _ = txn;
        self.posting_backend_base_loads += 1;
        return try self.alloc.dupe(u8, try self.postingBackendBaseData(posting_id, is_not_found));
    }

    pub fn loadPostingBackendCentroidDirectoryRecord(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !OwnedCentroidDirectoryRecord {
        _ = txn;
        var key_buf: [10]u8 = undefined;
        const data = self.getNamespaced(.{}, .nodes, hbc.encodeCentroidDirectoryKey(&key_buf, posting_id)) catch |err| {
            if (is_not_found(err)) return error.NotFound;
            return err;
        };
        return try CentroidDirectoryFormat.decode(self.alloc, data);
    }

    pub fn loadPostingBackendDeltaTail(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId) ![]PostingDeltaRecord {
        _ = txn;
        var out: std.ArrayList(PostingDeltaRecord) = .empty;
        errdefer out.deinit(self.alloc);
        for (self.delta_entries.items) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) continue;
            const decoded = try PostingFormat.decodeDeltaTail(self.alloc, entry.value);
            defer self.alloc.free(decoded);
            try out.appendSlice(self.alloc, decoded);
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn postingBackendDeltaTailStats(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, base_generation: u64) !PostingDeltaTailStats {
        _ = txn;
        var out = PostingDeltaTailStats{};
        for (self.delta_entries.items) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) continue;
            const stats = try PostingFormat.deltaTailStatsAfterGeneration(entry.value, base_generation);
            out.records += stats.records;
            out.records_after_generation += stats.records_after_generation;
            out.tombstones_after_generation += stats.tombstones_after_generation;
            out.max_sequence_after_generation = @max(out.max_sequence_after_generation, stats.max_sequence_after_generation);
            out.encoded_key_bytes += entry.key.len;
            out.encoded_value_bytes += entry.value.len;
        }
        return out;
    }

    pub fn postingBackendLatestDeltaOpAfterGenerationForMember(
        self: *PostingPersistenceTestIndex,
        txn: anytype,
        posting_id: PostingId,
        vector_id: VectorId,
        base_generation: u64,
    ) !?PostingDeltaOp {
        _ = txn;
        var best_sequence: u64 = 0;
        var best_op: ?PostingDeltaOp = null;
        for (self.delta_entries.items) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) continue;
            var iterator = try PostingFormat.DeltaTailIterator.init(entry.value);
            while (try iterator.next()) |record| {
                if (record.vector_id != vector_id) continue;
                if (PostingFormat.deltaSequenceGeneration(record.sequence) <= base_generation) continue;
                if (best_op == null or record.sequence >= best_sequence) {
                    best_sequence = record.sequence;
                    best_op = record.op;
                }
            }
        }
        return best_op;
    }

    pub fn applyPostingBackendDeltaTailIntoScratch(
        self: *PostingPersistenceTestIndex,
        txn: anytype,
        posting_id: PostingId,
        alloc: std.mem.Allocator,
        scratch: anytype,
        member_count: *usize,
        base_generation: u64,
    ) !DeltaReplayResult {
        _ = txn;
        var result = DeltaReplayResult{};
        for (self.delta_entries.items) |entry| {
            if (!hbc.postingDeltaKeyMatchesPosting(entry.key, posting_id)) continue;
            const entry_result = try PostingFormat.applyDeltaTailAfterGenerationIntoScratch(alloc, scratch, member_count, entry.value, base_generation);
            result.records += entry_result.records;
            result.max_sequence = @max(result.max_sequence, entry_result.max_sequence);
        }
        return result;
    }

    pub fn materializePostingBackendMembers(self: *PostingPersistenceTestIndex, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) ![]VectorId {
        self.posting_backend_member_materializations += 1;
        var base = try self.loadPostingBackendBase(txn, posting_id, is_not_found);
        defer base.deinit(self.alloc);
        const records = try self.loadPostingBackendDeltaTail(txn, posting_id);
        defer self.alloc.free(records);
        return try PostingFormat.materializeMembersAfterGeneration(self.alloc, base.members, records, base.generation);
    }

    pub fn foldPostingBackendDeltaTailIntoBase(
        self: *PostingPersistenceTestIndex,
        txn: anytype,
        posting_id: PostingId,
        is_not_found: fn (anyerror) bool,
        options: FoldDeltaTailOptions,
    ) !FoldDeltaTailResult {
        const base_header = try self.loadPostingBackendBaseHeader(txn, posting_id, is_not_found);
        const stats = try self.postingBackendDeltaTailStats(txn, posting_id, base_header.generation);
        if (!PostingStore.deltaTailShouldFold(base_header.member_count, stats, options)) {
            return .{
                .delta_records = stats.records,
                .base_member_count = base_header.member_count,
                .materialized_member_count = base_header.member_count,
                .next_generation = base_header.generation,
                .skipped = true,
            };
        }
        const materialized = try self.materializePostingBackendMembers(txn, posting_id, is_not_found);
        defer self.alloc.free(materialized);
        const next_generation = base_header.generation +| 1;
        try PostingStore.saveBase(self, txn, .{
            .posting_id = posting_id,
            .generation = next_generation,
            .members = materialized,
        });
        return .{
            .delta_records = stats.records,
            .base_member_count = base_header.member_count,
            .materialized_member_count = materialized.len,
            .written_base_value_bytes = self.base_value.len,
            .next_generation = next_generation,
        };
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
    posting_overlay_removed_members: std.AutoHashMapUnmanaged(VectorId, void) = .empty,
    posting_overlay_appended_positions: std.AutoHashMapUnmanaged(VectorId, usize) = .empty,
    posting_overlay_appended_ids: []VectorId = &.{},
    posting_overlay_appended_live: []bool = &.{},
    posting_overlay_appended_count: usize = 0,

    fn deinit(self: *PostingQueryMaterializeTestScratch, alloc: std.mem.Allocator) void {
        alloc.free(self.member_ids);
        self.posting_overlay_removed_members.deinit(alloc);
        self.posting_overlay_appended_positions.deinit(alloc);
        alloc.free(self.posting_overlay_appended_ids);
        alloc.free(self.posting_overlay_appended_live);
        self.* = .{};
    }

    pub fn ensureMemberIdCapacity(self: *PostingQueryMaterializeTestScratch, alloc: std.mem.Allocator, needed: usize) !void {
        if (self.member_ids.len < needed) self.member_ids = try alloc.realloc(self.member_ids, needed);
    }

    pub fn ensurePostingOverlayAppendCapacity(self: *PostingQueryMaterializeTestScratch, alloc: std.mem.Allocator, needed: usize) !void {
        if (self.posting_overlay_appended_ids.len < needed) {
            self.posting_overlay_appended_ids = try alloc.realloc(self.posting_overlay_appended_ids, needed);
        }
        if (self.posting_overlay_appended_live.len < needed) {
            self.posting_overlay_appended_live = try alloc.realloc(self.posting_overlay_appended_live, needed);
        }
    }

    pub fn resetPostingOverlayApply(self: *PostingQueryMaterializeTestScratch) void {
        self.posting_overlay_removed_members.clearRetainingCapacity();
        self.posting_overlay_appended_positions.clearRetainingCapacity();
        self.posting_overlay_appended_count = 0;
    }
};

const PostingQueryMaterializeTestProfile = struct {
    posting_overlay_ns: u64 = 0,
    posting_overlay_calls: u64 = 0,
    posting_overlay_base_members: u64 = 0,
    posting_base_decode_ns: u64 = 0,
    posting_base_decode_members: u64 = 0,
    posting_delta_replay_ns: u64 = 0,
    posting_delta_replay_records: u64 = 0,
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
    const base_header = try PostingStore.loadBaseHeader(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(PostingId, 9), base_header.posting_id);
    try std.testing.expectEqual(@as(u64, 4), base_header.generation);
    try std.testing.expectEqual(members.len, base_header.member_count);
    const base_stats = try PostingStore.loadBaseStats(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(PostingId, 9), base_stats.header.posting_id);
    try std.testing.expectEqual(@as(u64, 4), base_stats.header.generation);
    try std.testing.expectEqual(members.len, base_stats.header.member_count);
    try std.testing.expectEqual(@as(usize, 1), base_stats.block_count);
    try std.testing.expectEqual(index.base_value.len, base_stats.encoded_len);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_base_put_calls);
    try std.testing.expectEqual(@as(u64, members.len), index.write_profile.posting_base_members);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_base_blocks);

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
        .sequence = (@as(u64, 1) << 32) | 12,
        .op = .tombstone,
        .vector_id = 100,
    });
    try std.testing.expect(index.saw_append);

    var expected_delta_key: [18]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), index.delta_entries.items.len);
    try std.testing.expectEqualSlices(u8, hbc.encodePostingDeltaKey(&expected_delta_key, 9, (@as(u64, 1) << 32) | 12), index.delta_entries.items[0].key);
    const decoded = try PostingFormat.decodeDeltaTail(alloc, index.delta_entries.items[0].value);
    defer alloc.free(decoded);
    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(PostingDeltaOp.tombstone, decoded[0].op);
    try std.testing.expectEqual(@as(VectorId, 100), decoded[0].vector_id);
    const delta_stats = try PostingStore.deltaTailStats(&index, &txn, 9, std.math.maxInt(u64));
    try std.testing.expectEqual(@as(usize, 1), delta_stats.records);
    try std.testing.expectEqual(@as(usize, 0), delta_stats.records_after_generation);
    try std.testing.expectEqual(@as(u64, 0), delta_stats.max_sequence_after_generation);
    try std.testing.expectEqual(index.delta_entries.items[0].key.len, delta_stats.encoded_key_bytes);
    try std.testing.expectEqual(index.delta_entries.items[0].value.len, delta_stats.encoded_value_bytes);
    const live_delta_stats = try PostingStore.deltaTailStats(&index, &txn, 9, 0);
    try std.testing.expectEqual(@as(usize, 1), live_delta_stats.records_after_generation);
    try std.testing.expectEqual((@as(u64, 1) << 32) | 12, live_delta_stats.max_sequence_after_generation);
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
    try std.testing.expectEqual(try PostingFormat.encodedBaseSizeForMembers(&[_]VectorId{ 200, 300, 400 }), folded.written_base_value_bytes);
    try std.testing.expect(folded.peak_scratch_bytes > 0);
    try std.testing.expectEqual(@as(usize, 0), index.delta_entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_delta_fold_calls);
    try std.testing.expectEqual(@as(u64, records.len), index.write_profile.posting_delta_fold_records);
    try std.testing.expectEqual(@as(u64, 1), index.write_profile.posting_delta_fold_deleted_tail_keys);
    try std.testing.expectEqual(@as(u64, 18), index.write_profile.posting_delta_fold_deleted_tail_key_bytes);
    try std.testing.expect(index.write_profile.posting_delta_fold_deleted_tail_value_bytes > 0);
    try std.testing.expectEqual(@as(u64, 10), index.write_profile.posting_delta_fold_written_base_key_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(folded.written_base_value_bytes)), index.write_profile.posting_delta_fold_written_base_value_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(folded.peak_scratch_bytes)), index.write_profile.posting_delta_fold_peak_scratch_bytes);
}

test "posting store chunks grouped delta records by encoded value target" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{
            .dims = 2,
            .max_posting_delta_tail_value_bytes = 24,
        },
    };
    defer index.deinit();
    var txn = struct {}{};

    const records = [_]PostingDeltaRecord{
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .insert, .vector_id = 1 },
        .{ .sequence = (@as(u64, 5) << 32) | 2, .op = .insert, .vector_id = 2 },
        .{ .sequence = (@as(u64, 5) << 32) | 3, .op = .insert, .vector_id = 3 },
        .{ .sequence = (@as(u64, 5) << 32) | 4, .op = .insert, .vector_id = 4 },
    };
    try PostingStore.appendDeltaRecords(&index, &txn, 9, records[0..]);
    try std.testing.expectEqual(@as(usize, 2), index.delta_entries.items.len);

    const deltas = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas);
    try std.testing.expectEqual(@as(usize, records.len), deltas.len);
    for (records, 0..) |record, i| {
        try std.testing.expectEqual(record.sequence, deltas[i].sequence);
        try std.testing.expectEqual(record.op, deltas[i].op);
        try std.testing.expectEqual(record.vector_id, deltas[i].vector_id);
    }
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
    const fixed_width_base_bytes = PostingFormat.encoded_base_header_size + folded.materialized_member_count * @sizeOf(VectorId);
    try std.testing.expect(folded.written_base_value_bytes >= PostingFormat.encoded_base_header_size);
    try std.testing.expect(folded.written_base_value_bytes < fixed_width_base_bytes);
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
    try std.testing.expectEqual(@as(u64, @intCast(folded.written_base_value_bytes)), index.write_profile.posting_delta_fold_written_base_value_bytes);
    try std.testing.expectEqual(@as(u64, @intCast(folded.peak_scratch_bytes)), index.write_profile.posting_delta_fold_peak_scratch_bytes);
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
    try std.testing.expectEqual(try PostingFormat.encodedBaseSizeForMembers(&[_]VectorId{ 100, 300, 400 }), folded.written_base_value_bytes);
    try std.testing.expectEqual(@as(u64, 5), folded.next_generation);

    var base_after_fold = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_fold.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), base_after_fold.generation);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 300, 400, 100 }, base_after_fold.members);
    const deltas_after_fold = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after_fold);
    try std.testing.expectEqual(@as(usize, 0), deltas_after_fold.len);
}

test "posting store skips delta fold when materialized member cap would be exceeded" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    const members = [_]VectorId{ 100, 200, 300 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 1,
        .op = .insert,
        .vector_id = 400,
    });

    const skipped = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 1,
        .max_materialized_members = 3,
    });
    try std.testing.expect(skipped.skipped);
    try std.testing.expectEqual(@as(usize, 1), skipped.delta_records);
    try std.testing.expectEqual(@as(u64, 4), skipped.next_generation);

    var base_after_skip = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_skip.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 4), base_after_skip.generation);
    try std.testing.expectEqualSlices(VectorId, members[0..], base_after_skip.members);

    const deltas_after_skip = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after_skip);
    try std.testing.expectEqual(@as(usize, 1), deltas_after_skip.len);
}

test "posting store can fold delta tail based on encoded value bytes" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    const members = [_]VectorId{ 100, 200, 300 };
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });
    try PostingStore.appendDelta(&index, &txn, 9, .{
        .sequence = (@as(u64, 5) << 32) | 1,
        .op = .insert,
        .vector_id = 400,
    });

    const skipped = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 99,
        .min_tombstone_records = 99,
        .min_delta_to_base_ratio_bps = 20_000,
    });
    try std.testing.expect(skipped.skipped);

    const folded = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 99,
        .min_tombstone_records = 99,
        .min_delta_to_base_ratio_bps = 20_000,
        .min_delta_value_bytes = 1,
    });
    try std.testing.expect(!folded.skipped);
    try std.testing.expectEqual(@as(usize, 1), folded.delta_records);
    try std.testing.expectEqual(@as(u64, 5), folded.next_generation);

    var base_after_fold = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_fold.deinit(alloc);
    try std.testing.expectEqualSlices(VectorId, &[_]VectorId{ 100, 200, 300, 400 }, base_after_fold.members);
    const deltas_after_fold = try PostingStore.loadDeltaTail(&index, &txn, 9);
    defer alloc.free(deltas_after_fold);
    try std.testing.expectEqual(@as(usize, 0), deltas_after_fold.len);
}

test "posting store indexed delta fold compacts tombstones and repeated inserts" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{ .alloc = alloc };
    defer index.deinit();
    var txn = struct {}{};

    var members: [130]VectorId = undefined;
    for (&members, 0..) |*member, i| member.* = @intCast(i + 1);
    try PostingStore.saveBase(&index, &txn, .{
        .posting_id = 9,
        .generation = 4,
        .members = members[0..],
    });

    try PostingStore.appendDeltaRecords(&index, &txn, 9, &.{
        .{ .sequence = (@as(u64, 5) << 32) | 1, .op = .tombstone, .vector_id = 2 },
        .{ .sequence = (@as(u64, 5) << 32) | 2, .op = .insert, .vector_id = 200 },
        .{ .sequence = (@as(u64, 5) << 32) | 3, .op = .insert, .vector_id = 200 },
        .{ .sequence = (@as(u64, 5) << 32) | 4, .op = .replace, .vector_id = 3 },
        .{ .sequence = (@as(u64, 5) << 32) | 5, .op = .tombstone, .vector_id = 200 },
        .{ .sequence = (@as(u64, 5) << 32) | 6, .op = .insert, .vector_id = 201 },
    });

    const folded = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 1,
    });
    try std.testing.expect(!folded.skipped);

    var base_after_fold = try PostingStore.loadBase(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer base_after_fold.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), base_after_fold.generation);
    try std.testing.expectEqual(@as(usize, 130), base_after_fold.members.len);
    try std.testing.expect(std.mem.indexOfScalar(VectorId, base_after_fold.members, 2) == null);
    try std.testing.expect(std.mem.indexOfScalar(VectorId, base_after_fold.members, 200) == null);
    try std.testing.expectEqual(@as(VectorId, 3), base_after_fold.members[base_after_fold.members.len - 2]);
    try std.testing.expectEqual(@as(VectorId, 201), base_after_fold.members[base_after_fold.members.len - 1]);
}

test "posting store segment backend hooks route posting persistence" {
    const alloc = std.testing.allocator;
    var index = PostingPersistenceTestIndex{
        .alloc = alloc,
        .config = .{
            .dims = 2,
            .posting_storage_mode = .base_delta,
            .posting_backend = .segments,
        },
    };
    defer index.deinit();
    var txn = struct {}{};

    try PostingStore.saveBaseAndCentroidDirectoryRecord(&index, &txn, .{
        .posting_id = 9,
        .generation = 1,
        .members = &.{ 10, 20 },
    }, .{
        .posting_id = 9,
        .generation = 1,
        .mutation_version = 1,
        .payload_version = 1,
        .flags = CentroidDirectoryFormat.dirty_flag,
        .parent = 1,
        .level = 0,
        .member_count = 2,
        .bounds_radius = 1.5,
        .centroid = &.{ 1.0, 2.0 },
    });
    try PostingStore.appendDeltaRecords(&index, &txn, 9, &.{
        .{ .sequence = (@as(u64, 2) << 32) | 1, .op = .tombstone, .vector_id = 10 },
        .{ .sequence = (@as(u64, 2) << 32) | 2, .op = .insert, .vector_id = 30 },
    });

    try std.testing.expectEqual(@as(u64, 1), index.posting_backend_base_saves);
    try std.testing.expectEqual(@as(u64, 1), index.posting_backend_centroid_saves);
    try std.testing.expectEqual(@as(u64, 1), index.posting_backend_delta_appends);
    try std.testing.expectEqual(@as(u64, 0), index.cursor_open_count);
    try std.testing.expectError(error.UnsupportedPostingBackendBorrowedData, PostingStore.loadBaseData(&index, &txn, 9, isNotFoundForPostingPersistenceTest));

    const header = try PostingStore.loadBaseHeader(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    try std.testing.expectEqual(@as(u64, 1), header.generation);
    try std.testing.expectEqual(@as(u32, 2), header.member_count);

    var centroid = try PostingStore.loadCentroidDirectoryRecord(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer centroid.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), centroid.mutation_version);
    try std.testing.expectEqual(CentroidDirectoryFormat.dirty_flag, centroid.flags);

    const stats = try PostingStore.deltaTailStats(&index, &txn, 9, 1);
    try std.testing.expectEqual(@as(usize, 2), stats.records);
    try std.testing.expectEqual(@as(usize, 2), stats.records_after_generation);
    try std.testing.expectEqual(@as(usize, 1), stats.tombstones_after_generation);
    try std.testing.expect(!try PostingStore.containsBaseDeltaMember(&index, &txn, 9, 10, isNotFoundForPostingPersistenceTest));
    try std.testing.expect(try PostingStore.containsBaseDeltaMember(&index, &txn, 9, 20, isNotFoundForPostingPersistenceTest));
    try std.testing.expect(try PostingStore.containsBaseDeltaMember(&index, &txn, 9, 30, isNotFoundForPostingPersistenceTest));
    try std.testing.expect(!try PostingStore.containsBaseDeltaMember(&index, &txn, 9, 40, isNotFoundForPostingPersistenceTest));

    const members = try PostingStore.materializeBaseDeltaMembers(&index, &txn, 9, isNotFoundForPostingPersistenceTest);
    defer alloc.free(members);
    try std.testing.expectEqualSlices(VectorId, &.{ 20, 30 }, members);

    var scratch = PostingQueryMaterializeTestScratch{};
    defer scratch.deinit(alloc);
    var profile = PostingQueryMaterializeTestProfile{};
    const query_members = try PostingStore.copyQueryMemberIds(
        &index,
        &txn,
        alloc,
        &scratch,
        .{
            .id = 9,
            .parent = 1,
            .level = 0,
            .centroid = &.{ 1.0, 2.0 },
            .members = &.{},
            .state = .{ .mutation_version = 2 },
        },
        &profile,
        postingQueryTestNow,
        postingQueryTestElapsed,
    );
    try std.testing.expectEqualSlices(VectorId, &.{ 20, 30 }, query_members);
    try std.testing.expectEqual(@as(u64, 0), index.cursor_open_count);
    try std.testing.expectEqual(@as(u64, 0), index.posting_backend_member_materializations);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_delta_replay_records);

    const folded = try PostingStore.foldDeltaTailIntoBaseWithOptions(&index, &txn, 9, isNotFoundForPostingPersistenceTest, .{
        .min_delta_records = 1,
    });
    try std.testing.expectEqual(@as(usize, 2), folded.delta_records);
    try std.testing.expectEqual(@as(usize, 2), folded.materialized_member_count);
    try std.testing.expectEqual(@as(u64, 2), folded.next_generation);
    try std.testing.expectEqual(@as(u64, 0), index.cursor_open_count);
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
    try std.testing.expectEqual(@as(u64, 15), profile.posting_base_decode_ns);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_base_decode_members);
    try std.testing.expectEqual(@as(u64, 15), profile.posting_delta_replay_ns);
    try std.testing.expectEqual(@as(u64, 2), profile.posting_delta_replay_records);
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
    try std.testing.expectEqual(@as(u64, 3), profile.posting_base_decode_members);
    try std.testing.expectEqual(@as(u64, 0), profile.posting_delta_replay_records);
    try std.testing.expectEqual(@as(u64, 0), profile.posting_overlay_delta_records);
    try std.testing.expectEqual(@as(u64, 1), profile.posting_overlay_delta_scan_skips);
    try std.testing.expectEqual(@as(u64, 3), profile.posting_overlay_materialized_members);
}
