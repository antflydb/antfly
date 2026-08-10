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

//! Storage-free binary contract for bounded data-Raft projection results.
//! Large pages cross the compiled kernel boundary once as an owned buffer;
//! neither physical DB types nor one callback per record leak to consumers.

const std = @import("std");

const magic = "AFRP";
const format_version: u8 = 1;

const Kind = enum(u8) {
    split_control = 1,
    handoff_metadata = 2,
    group_state_page = 3,
    split_deltas = 4,
    byte_range = 5,
};

pub const SplitPhase = enum(u8) {
    none = 0,
    prepare = 1,
    splitting = 2,
    finalizing = 3,
    rolling_back = 4,
};

pub const SplitTerminalOutcome = enum(u8) {
    finalized = 1,
    rolled_back = 2,
};

pub const ByteRange = struct {
    start: []u8,
    end: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeBytes(alloc, self.start);
        freeBytes(alloc, self.end);
        self.* = undefined;
    }
};

pub const SplitState = struct {
    phase: SplitPhase,
    transition_id: u64,
    attempt_epoch: u64,
    split_key: []u8,
    new_shard_id: u64,
    original_range_end: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeBytes(alloc, self.split_key);
        freeBytes(alloc, self.original_range_end);
        self.* = undefined;
    }
};

pub const SplitTerminal = struct {
    transition_id: u64,
    attempt_epoch: u64,
    destination_group_id: u64,
    split_key: []u8,
    outcome: SplitTerminalOutcome,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeBytes(alloc, self.split_key);
        self.* = undefined;
    }
};

pub const SplitAcknowledgement = struct {
    transition_id: u64,
    attempt_epoch: u64,
    destination_group_id: u64,
    delta_sequence: u64,
};

pub const SplitControlObservation = struct {
    state: ?SplitState = null,
    terminal: ?SplitTerminal = null,
    acknowledgement: ?SplitAcknowledgement = null,
    delta_sequence: u64 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.state) |*value| value.deinit(alloc);
        if (self.terminal) |*value| value.deinit(alloc);
        self.* = undefined;
    }
};

pub const SplitHandoffMetadata = struct {
    byte_range: ByteRange,
    split_state: SplitState,
    base_delta_sequence: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.byte_range.deinit(alloc);
        self.split_state.deinit(alloc);
        self.* = undefined;
    }
};

pub const KeyValue = struct {
    key: []u8,
    value: []u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeBytes(alloc, self.key);
        freeBytes(alloc, self.value);
        self.* = undefined;
    }
};

pub const GroupStatePage = struct {
    entries: []KeyValue,
    exhausted: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        self.* = undefined;
    }
};

pub const SplitDelta = struct {
    sequence: u64,
    timestamp: u64,
    writes: []KeyValue,
    deletes: [][]u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.writes) |*write| write.deinit(alloc);
        alloc.free(self.writes);
        for (self.deletes) |key| freeBytes(alloc, key);
        alloc.free(self.deletes);
        self.* = undefined;
    }
};

pub const SplitDeltas = struct {
    items: []SplitDelta,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items) |*delta| delta.deinit(alloc);
        alloc.free(self.items);
        self.* = undefined;
    }
};

pub fn encodeSplitControlAlloc(alloc: std.mem.Allocator, observation: anytype) ![]u8 {
    var writer = try Writer.init(alloc, .split_control);
    errdefer writer.deinit();
    var flags: u8 = 0;
    if (observation.state != null) flags |= 1;
    if (observation.terminal != null) flags |= 2;
    if (observation.acknowledgement != null) flags |= 4;
    try writer.byte(flags);
    try writer.int(u64, observation.delta_sequence);
    if (observation.state) |state| try writeSplitState(&writer, state);
    if (observation.terminal) |terminal| try writeSplitTerminal(&writer, terminal);
    if (observation.acknowledgement) |ack| try writeSplitAcknowledgement(&writer, ack);
    return try writer.finish();
}

pub fn decodeSplitControlAlloc(alloc: std.mem.Allocator, encoded: []const u8) !SplitControlObservation {
    var reader = try Reader.init(encoded, .split_control);
    const flags = try reader.byte();
    if (flags & ~@as(u8, 7) != 0) return error.InvalidProjectionWire;
    var result = SplitControlObservation{ .delta_sequence = try reader.int(u64) };
    errdefer result.deinit(alloc);
    if (flags & 1 != 0) result.state = try readSplitState(&reader, alloc);
    if (flags & 2 != 0) result.terminal = try readSplitTerminal(&reader, alloc);
    if (flags & 4 != 0) result.acknowledgement = try readSplitAcknowledgement(&reader);
    try reader.finish();
    return result;
}

pub fn encodeHandoffMetadataAlloc(alloc: std.mem.Allocator, handoff: anytype) ![]u8 {
    var writer = try Writer.init(alloc, .handoff_metadata);
    errdefer writer.deinit();
    try writeRange(&writer, handoff.byte_range);
    try writeSplitState(&writer, handoff.split_state);
    try writer.int(u64, handoff.base_delta_sequence);
    return try writer.finish();
}

pub fn decodeHandoffMetadataAlloc(alloc: std.mem.Allocator, encoded: []const u8) !SplitHandoffMetadata {
    var reader = try Reader.init(encoded, .handoff_metadata);
    var byte_range = try readRange(&reader, alloc);
    errdefer byte_range.deinit(alloc);
    var split_state = try readSplitState(&reader, alloc);
    errdefer split_state.deinit(alloc);
    const result = SplitHandoffMetadata{
        .byte_range = byte_range,
        .split_state = split_state,
        .base_delta_sequence = try reader.int(u64),
    };
    try reader.finish();
    return result;
}

pub fn encodeRangeAlloc(alloc: std.mem.Allocator, byte_range: anytype) ![]u8 {
    var writer = try Writer.init(alloc, .byte_range);
    errdefer writer.deinit();
    try writeRange(&writer, byte_range);
    return try writer.finish();
}

pub fn decodeRangeAlloc(alloc: std.mem.Allocator, encoded: []const u8) !ByteRange {
    var reader = try Reader.init(encoded, .byte_range);
    var result = try readRange(&reader, alloc);
    errdefer result.deinit(alloc);
    try reader.finish();
    return result;
}

pub fn encodeGroupStatePageAlloc(alloc: std.mem.Allocator, page: anytype) ![]u8 {
    var writer = try Writer.init(alloc, .group_state_page);
    errdefer writer.deinit();
    try writer.byte(@intFromBool(page.exhausted));
    try writer.count(page.entries.len);
    for (page.entries) |entry| {
        try writer.bytes(entry.key);
        try writer.bytes(entry.value);
    }
    return try writer.finish();
}

pub fn decodeGroupStatePageAlloc(alloc: std.mem.Allocator, encoded: []const u8) !GroupStatePage {
    var reader = try Reader.init(encoded, .group_state_page);
    const exhausted_raw = try reader.byte();
    if (exhausted_raw > 1) return error.InvalidProjectionWire;
    const count = try reader.count();
    const entries = try alloc.alloc(KeyValue, count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    while (initialized < entries.len) : (initialized += 1) {
        const key = try reader.bytesAlloc(alloc);
        errdefer freeBytes(alloc, key);
        entries[initialized] = .{
            .key = key,
            .value = try reader.bytesAlloc(alloc),
        };
    }
    try reader.finish();
    return .{ .entries = entries, .exhausted = exhausted_raw != 0 };
}

pub fn encodeSplitDeltasAlloc(alloc: std.mem.Allocator, deltas: anytype) ![]u8 {
    var writer = try Writer.init(alloc, .split_deltas);
    errdefer writer.deinit();
    try writer.count(deltas.len);
    for (deltas) |delta| {
        try writer.int(u64, delta.sequence);
        try writer.int(u64, delta.timestamp);
        try writer.count(delta.writes.len);
        try writer.count(delta.deletes.len);
        for (delta.writes) |write| {
            try writer.bytes(write.key);
            try writer.bytes(write.value);
        }
        for (delta.deletes) |key| try writer.bytes(key);
    }
    return try writer.finish();
}

pub fn decodeSplitDeltasAlloc(alloc: std.mem.Allocator, encoded: []const u8) !SplitDeltas {
    var reader = try Reader.init(encoded, .split_deltas);
    const items = try alloc.alloc(SplitDelta, try reader.count());
    var initialized: usize = 0;
    errdefer {
        for (items[0..initialized]) |*delta| delta.deinit(alloc);
        alloc.free(items);
    }
    while (initialized < items.len) : (initialized += 1) {
        const sequence = try reader.int(u64);
        const timestamp = try reader.int(u64);
        const write_count = try reader.count();
        const delete_count = try reader.count();
        const writes = try alloc.alloc(KeyValue, write_count);
        var writes_initialized: usize = 0;
        errdefer {
            for (writes[0..writes_initialized]) |*write| write.deinit(alloc);
            alloc.free(writes);
        }
        while (writes_initialized < writes.len) : (writes_initialized += 1) {
            const key = try reader.bytesAlloc(alloc);
            errdefer freeBytes(alloc, key);
            writes[writes_initialized] = .{
                .key = key,
                .value = try reader.bytesAlloc(alloc),
            };
        }
        const deletes = try alloc.alloc([]u8, delete_count);
        var deletes_initialized: usize = 0;
        errdefer {
            for (deletes[0..deletes_initialized]) |key| freeBytes(alloc, key);
            alloc.free(deletes);
        }
        while (deletes_initialized < deletes.len) : (deletes_initialized += 1)
            deletes[deletes_initialized] = try reader.bytesAlloc(alloc);
        items[initialized] = .{
            .sequence = sequence,
            .timestamp = timestamp,
            .writes = writes,
            .deletes = deletes,
        };
    }
    try reader.finish();
    return .{ .items = items };
}

fn writeRange(writer: *Writer, byte_range: anytype) !void {
    try writer.bytes(byte_range.start);
    try writer.bytes(byte_range.end);
}

fn readRange(reader: *Reader, alloc: std.mem.Allocator) !ByteRange {
    const start = try reader.bytesAlloc(alloc);
    errdefer freeBytes(alloc, start);
    return .{ .start = start, .end = try reader.bytesAlloc(alloc) };
}

fn writeSplitState(writer: *Writer, state: anytype) !void {
    try writer.byte(@intFromEnum(state.phase));
    try writer.int(u64, state.transition_id);
    try writer.int(u64, state.attempt_epoch);
    try writer.bytes(state.split_key);
    try writer.int(u64, state.new_shard_id);
    try writer.bytes(state.original_range_end);
}

fn readSplitState(reader: *Reader, alloc: std.mem.Allocator) !SplitState {
    const phase = std.enums.fromInt(SplitPhase, try reader.byte()) orelse return error.InvalidProjectionWire;
    const transition_id = try reader.int(u64);
    const attempt_epoch = try reader.int(u64);
    const split_key = try reader.bytesAlloc(alloc);
    errdefer freeBytes(alloc, split_key);
    const new_shard_id = try reader.int(u64);
    return .{
        .phase = phase,
        .transition_id = transition_id,
        .attempt_epoch = attempt_epoch,
        .split_key = split_key,
        .new_shard_id = new_shard_id,
        .original_range_end = try reader.bytesAlloc(alloc),
    };
}

fn writeSplitTerminal(writer: *Writer, terminal: anytype) !void {
    try writer.int(u64, terminal.transition_id);
    try writer.int(u64, terminal.attempt_epoch);
    try writer.int(u64, terminal.destination_group_id);
    try writer.bytes(terminal.split_key);
    try writer.byte(@intFromEnum(terminal.outcome));
}

fn readSplitTerminal(reader: *Reader, alloc: std.mem.Allocator) !SplitTerminal {
    const transition_id = try reader.int(u64);
    const attempt_epoch = try reader.int(u64);
    const destination_group_id = try reader.int(u64);
    const split_key = try reader.bytesAlloc(alloc);
    errdefer freeBytes(alloc, split_key);
    const outcome = std.enums.fromInt(SplitTerminalOutcome, try reader.byte()) orelse
        return error.InvalidProjectionWire;
    return .{
        .transition_id = transition_id,
        .attempt_epoch = attempt_epoch,
        .destination_group_id = destination_group_id,
        .split_key = split_key,
        .outcome = outcome,
    };
}

fn writeSplitAcknowledgement(writer: *Writer, acknowledgement: anytype) !void {
    try writer.int(u64, acknowledgement.transition_id);
    try writer.int(u64, acknowledgement.attempt_epoch);
    try writer.int(u64, acknowledgement.destination_group_id);
    try writer.int(u64, acknowledgement.delta_sequence);
}

fn readSplitAcknowledgement(reader: *Reader) !SplitAcknowledgement {
    return .{
        .transition_id = try reader.int(u64),
        .attempt_epoch = try reader.int(u64),
        .destination_group_id = try reader.int(u64),
        .delta_sequence = try reader.int(u64),
    };
}

const Writer = struct {
    alloc: std.mem.Allocator,
    bytes_list: std.ArrayListUnmanaged(u8) = .empty,

    fn init(alloc: std.mem.Allocator, kind: Kind) !Writer {
        var self = Writer{ .alloc = alloc };
        errdefer self.deinit();
        try self.bytes_list.appendSlice(alloc, magic);
        try self.bytes_list.append(alloc, format_version);
        try self.bytes_list.append(alloc, @intFromEnum(kind));
        return self;
    }

    fn deinit(self: *Writer) void {
        self.bytes_list.deinit(self.alloc);
    }

    fn finish(self: *Writer) ![]u8 {
        return try self.bytes_list.toOwnedSlice(self.alloc);
    }

    fn byte(self: *Writer, value: u8) !void {
        try self.bytes_list.append(self.alloc, value);
    }

    fn int(self: *Writer, comptime T: type, value: T) !void {
        var encoded: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &encoded, value, .little);
        try self.bytes_list.appendSlice(self.alloc, &encoded);
    }

    fn count(self: *Writer, value: usize) !void {
        if (value > std.math.maxInt(u32)) return error.ProjectionWireTooLarge;
        try self.int(u32, @intCast(value));
    }

    fn bytes(self: *Writer, value: []const u8) !void {
        try self.count(value.len);
        try self.bytes_list.appendSlice(self.alloc, value);
    }
};

const Reader = struct {
    encoded: []const u8,
    position: usize,

    fn init(encoded: []const u8, expected: Kind) !Reader {
        if (encoded.len < magic.len + 2 or
            !std.mem.eql(u8, encoded[0..magic.len], magic) or
            encoded[magic.len] != format_version or
            encoded[magic.len + 1] != @intFromEnum(expected))
            return error.InvalidProjectionWire;
        return .{ .encoded = encoded, .position = magic.len + 2 };
    }

    fn finish(self: *Reader) !void {
        if (self.position != self.encoded.len) return error.InvalidProjectionWire;
    }

    fn byte(self: *Reader) !u8 {
        if (self.position >= self.encoded.len) return error.InvalidProjectionWire;
        defer self.position += 1;
        return self.encoded[self.position];
    }

    fn int(self: *Reader, comptime T: type) !T {
        if (@sizeOf(T) > self.encoded.len - self.position) return error.InvalidProjectionWire;
        const value = std.mem.readInt(T, self.encoded[self.position..][0..@sizeOf(T)], .little);
        self.position += @sizeOf(T);
        return value;
    }

    fn count(self: *Reader) !usize {
        return @intCast(try self.int(u32));
    }

    fn bytesAlloc(self: *Reader, alloc: std.mem.Allocator) ![]u8 {
        const len = try self.count();
        if (len > self.encoded.len - self.position) return error.InvalidProjectionWire;
        const result = try alloc.dupe(u8, self.encoded[self.position..][0..len]);
        self.position += len;
        return result;
    }
};

fn freeBytes(alloc: std.mem.Allocator, value: []u8) void {
    if (value.len > 0) alloc.free(value);
}

test "projection wire round trips bounded pages and deltas" {
    const alloc = std.testing.allocator;
    const page = .{
        .entries = &[_]struct { key: []const u8, value: []const u8 }{
            .{ .key = "a", .value = "one" },
            .{ .key = "b", .value = "two" },
        },
        .exhausted = false,
    };
    const encoded_page = try encodeGroupStatePageAlloc(alloc, page);
    defer alloc.free(encoded_page);
    var decoded_page = try decodeGroupStatePageAlloc(alloc, encoded_page);
    defer decoded_page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), decoded_page.entries.len);
    try std.testing.expectEqualStrings("two", decoded_page.entries[1].value);
    try std.testing.expect(!decoded_page.exhausted);

    const deltas = &[_]struct {
        sequence: u64,
        timestamp: u64,
        writes: []const struct { key: []const u8, value: []const u8 },
        deletes: []const []const u8,
    }{.{
        .sequence = 8,
        .timestamp = 9,
        .writes = &.{.{ .key = "k", .value = "v" }},
        .deletes = &.{"gone"},
    }};
    const encoded_deltas = try encodeSplitDeltasAlloc(alloc, deltas);
    defer alloc.free(encoded_deltas);
    var decoded_deltas = try decodeSplitDeltasAlloc(alloc, encoded_deltas);
    defer decoded_deltas.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 8), decoded_deltas.items[0].sequence);
    try std.testing.expectEqualStrings("gone", decoded_deltas.items[0].deletes[0]);
}

test "projection wire round trips split control and handoff" {
    const alloc = std.testing.allocator;
    const state = SplitState{
        .phase = SplitPhase.splitting,
        .transition_id = @as(u64, 11),
        .attempt_epoch = @as(u64, 12),
        .split_key = @constCast("m"),
        .new_shard_id = @as(u64, 13),
        .original_range_end = @constCast("z"),
    };
    const observation = .{
        .state = @as(?SplitState, state),
        .terminal = @as(?SplitTerminal, null),
        .acknowledgement = @as(?SplitAcknowledgement, .{
            .transition_id = 11,
            .attempt_epoch = 12,
            .destination_group_id = 13,
            .delta_sequence = 14,
        }),
        .delta_sequence = @as(u64, 15),
    };
    const encoded = try encodeSplitControlAlloc(alloc, observation);
    defer alloc.free(encoded);
    var decoded = try decodeSplitControlAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(SplitPhase.splitting, decoded.state.?.phase);
    try std.testing.expectEqualStrings("m", decoded.state.?.split_key);
    try std.testing.expectEqual(@as(u64, 14), decoded.acknowledgement.?.delta_sequence);

    const handoff = .{
        .byte_range = .{ .start = @as([]const u8, "a"), .end = @as([]const u8, "z") },
        .split_state = state,
        .base_delta_sequence = @as(u64, 19),
    };
    const encoded_handoff = try encodeHandoffMetadataAlloc(alloc, handoff);
    defer alloc.free(encoded_handoff);
    var decoded_handoff = try decodeHandoffMetadataAlloc(alloc, encoded_handoff);
    defer decoded_handoff.deinit(alloc);
    try std.testing.expectEqualStrings("a", decoded_handoff.byte_range.start);
    try std.testing.expectEqualStrings("z", decoded_handoff.byte_range.end);
    try std.testing.expectEqual(@as(u64, 19), decoded_handoff.base_delta_sequence);
}

test "projection wire rejects truncation and trailing bytes" {
    const alloc = std.testing.allocator;
    const encoded = try encodeRangeAlloc(alloc, .{ .start = "a", .end = "z" });
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidProjectionWire, decodeRangeAlloc(alloc, encoded[0 .. encoded.len - 1]));
    const with_trailing = try std.mem.concat(alloc, u8, &.{ encoded, "x" });
    defer alloc.free(with_trailing);
    try std.testing.expectError(error.InvalidProjectionWire, decodeRangeAlloc(alloc, with_trailing));
}
