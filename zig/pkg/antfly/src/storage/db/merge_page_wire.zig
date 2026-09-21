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

//! Shared private HTTP, Raft projection and standby encoding. Chunks are
//! streamed as base64 with fixed scratch space, never numeric JSON arrays.
const std = @import("std");
const pages = @import("merge_page_contract.zig");
const binary = @import("relational_integrity_json.zig");
const Allocator = std.mem.Allocator;

const WireChunk = struct {
    row_key: []const u8,
    timestamp: u64,
    total_bytes: u64,
    row_digest: pages.Digest,
    offset: u64,
    data_base64: []const u8,
    chunk_digest: pages.Digest,
};

const WireCommand = struct {
    next_snapshot_position: ?pages.SnapshotPosition = null,
    source: pages.Source,
    sequence: u64,
    phase: pages.Phase,
    after: []const u8 = "",
    next: []const u8 = "",
    exhausted: bool,
    digest: pages.Digest,
    timestamps: []const u64 = &.{},
    tail: ?pages.Tail = null,
    chunk: ?WireChunk = null,
    integrity: []const pages.IntegrityEffect = &.{},
};

comptime {
    if (@typeInfo(WireCommand).@"struct".fields.len != @typeInfo(pages.Command).@"struct".fields.len)
        @compileError("update merge page wire projection for new command fields");
    if (@typeInfo(WireChunk).@"struct".fields.len != @typeInfo(pages.Chunk).@"struct".fields.len)
        @compileError("update merge page wire projection for new chunk fields");
}

pub fn write(command: pages.Command, stream: anytype) @TypeOf(stream.*).Error!void {
    try stream.beginObject();
    inline for (@typeInfo(pages.Command).@"struct".fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "next_snapshot_position")) {
            if (command.next_snapshot_position) |position| {
                try stream.objectField(field.name);
                try binary.write(position, stream);
            }
            continue;
        }
        if (comptime std.mem.eql(u8, field.name, "chunk")) {
            if (command.chunk) |chunk| {
                try stream.objectField("chunk");
                try stream.beginObject();
                inline for (@typeInfo(pages.Chunk).@"struct".fields) |chunk_field| {
                    if (comptime std.mem.eql(u8, chunk_field.name, "data")) {
                        try stream.objectField("data_base64");
                        try stream.beginWriteRaw();
                        try stream.writer.writeByte('"');
                        // Multiples of three ensure only the final block pads.
                        var buffer: [4096]u8 = undefined;
                        var offset: usize = 0;
                        while (offset < chunk.data.len) {
                            const end = @min(chunk.data.len, offset + 3072);
                            const encoded = std.base64.standard.Encoder.encode(&buffer, chunk.data[offset..end]);
                            try stream.writer.writeAll(encoded);
                            offset = end;
                        }
                        try stream.writer.writeByte('"');
                        stream.endWriteRaw();
                    } else {
                        try stream.objectField(chunk_field.name);
                        try binary.write(@field(chunk, chunk_field.name), stream);
                    }
                }
                try stream.endObject();
            }
        } else {
            try stream.objectField(field.name);
            try binary.write(@field(command, field.name), stream);
        }
    }
    try stream.endObject();
}

pub fn encodeAlloc(alloc: Allocator, command: pages.Command) ![]u8 {
    if (command.chunk) |chunk| if (chunk.data.len == 0 or chunk.data.len > pages.max_chunk_bytes) return error.InvalidMergePage;
    return std.json.Stringify.valueAlloc(alloc, command, .{});
}

pub fn parseFromValue(alloc: Allocator, value: std.json.Value) !std.json.Parsed(pages.Command) {
    var wire = try std.json.parseFromValue(WireCommand, alloc, value, .{ .allocate = .alloc_always });
    errdefer wire.deinit();
    return .{ .arena = wire.arena, .value = try fromWire(wire.arena.allocator(), wire.value) };
}

pub fn parse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!pages.Command {
    return fromWire(alloc, try std.json.innerParse(WireCommand, alloc, source, options));
}

pub fn parseValueLeaky(alloc: Allocator, value: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!pages.Command {
    return fromWire(alloc, try std.json.innerParseFromValue(WireCommand, alloc, value, options));
}

fn fromWire(alloc: Allocator, wire: WireCommand) std.json.ParseFromValueError!pages.Command {
    var command: pages.Command = undefined;
    inline for (@typeInfo(pages.Command).@"struct".fields) |field| {
        if (comptime !std.mem.eql(u8, field.name, "chunk")) @field(command, field.name) = @field(wire, field.name);
    }
    command.chunk = null;
    if (wire.chunk) |chunk| {
        if (chunk.data_base64.len > std.base64.standard.Encoder.calcSize(pages.max_chunk_bytes)) return error.LengthMismatch;
        const len = std.base64.standard.Decoder.calcSizeForSlice(chunk.data_base64) catch return error.UnexpectedToken;
        if (len == 0 or len > pages.max_chunk_bytes) return error.LengthMismatch;
        const bytes = try alloc.alloc(u8, len);
        std.base64.standard.Decoder.decode(bytes, chunk.data_base64) catch return error.UnexpectedToken;
        command.chunk = .{
            .row_key = chunk.row_key,
            .timestamp = chunk.timestamp,
            .total_bytes = chunk.total_bytes,
            .row_digest = chunk.row_digest,
            .offset = chunk.offset,
            .data = bytes,
            .chunk_digest = chunk.chunk_digest,
        };
    }
    return command;
}

test "merge page chunk wire preserves arbitrary bytes with bounded expansion" {
    const alloc = std.testing.allocator;
    const data = try alloc.alloc(u8, pages.max_chunk_bytes);
    defer alloc.free(data);
    @memset(data, 255);
    const command: pages.Command = .{
        .source = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .pin_digest = @splat(1), .applied_index = 4 },
        .sequence = 1,
        .phase = .rows,
        .exhausted = false,
        .digest = @splat(2),
        .chunk = .{ .row_key = &.{ 0, 255 }, .timestamp = 8, .total_bytes = data.len + 3, .row_digest = @splat(3), .offset = 0, .data = data, .chunk_digest = @splat(4) },
    };
    const encoded = try encodeAlloc(alloc, command);
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len < data.len * 3 / 2);
    // The streaming path has no allocator and uses fixed scratch independent
    // of payload length, including binary data at the maximum chunk size.
    var tiny: [1]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&tiny);
    try std.json.Stringify.value(command, .{}, &counter.writer);
    try std.testing.expectEqual(encoded.len, counter.fullCount());
    var value = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer value.deinit();
    var decoded = try parseFromValue(alloc, value.value);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, data, decoded.value.chunk.?.data);
    try std.testing.expectEqualSlices(u8, command.chunk.?.row_key, decoded.value.chunk.?.row_key);
    const field = value.value.object.getPtr("chunk").?.object.getPtr("data_base64").?;
    field.* = .{ .string = "!!!!" };
    try std.testing.expectError(error.UnexpectedToken, parseFromValue(alloc, value.value));
}
