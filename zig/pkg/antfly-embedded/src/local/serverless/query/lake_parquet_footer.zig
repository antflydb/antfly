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

//! Parquet footer trailer parsing and metadata range planning.

const std = @import("std");
const range_io = @import("lake_range_io.zig");

const parquet_magic = "PAR1";
const trailer_len = 8;

pub const FooterPreflight = struct {
    footer_metadata_len: u32,
    footer_metadata_range: range_io.ByteRange,
    metadata_tail_offset: ?usize = null,

    pub fn metadataAvailableInTail(self: FooterPreflight) bool {
        return self.metadata_tail_offset != null;
    }

    pub fn metadataSlice(self: FooterPreflight, tail_bytes: []const u8) ?[]const u8 {
        const offset = self.metadata_tail_offset orelse return null;
        const len: usize = @intCast(self.footer_metadata_len);
        if (offset > tail_bytes.len or len > tail_bytes.len - offset) return null;
        return tail_bytes[offset .. offset + len];
    }
};

pub fn parseFooterPreflight(
    object_len: u64,
    tail_offset: u64,
    tail_bytes: []const u8,
) !FooterPreflight {
    if (object_len < trailer_len) return error.InvalidParquetFooter;
    if (tail_offset > object_len) return error.InvalidParquetFooter;
    if (tail_bytes.len < trailer_len) return error.InvalidParquetFooter;
    if (tail_bytes.len != object_len - tail_offset) return error.InvalidParquetFooter;

    const magic_offset = tail_bytes.len - parquet_magic.len;
    if (!std.mem.eql(u8, tail_bytes[magic_offset..], parquet_magic)) return error.InvalidParquetFooterMagic;

    const len_offset = tail_bytes.len - trailer_len;
    const footer_metadata_len = std.mem.readInt(u32, tail_bytes[len_offset .. len_offset + 4][0..4], .little);
    if (footer_metadata_len == 0) return error.InvalidParquetFooter;
    if (footer_metadata_len > range_io.max_parquet_footer_metadata_bytes) return error.ParquetMetadataTooLarge;
    if (footer_metadata_len > object_len - trailer_len) return error.InvalidParquetFooter;

    const metadata_offset = object_len - trailer_len - footer_metadata_len;
    const range = range_io.ByteRange{
        .offset = metadata_offset,
        .len = footer_metadata_len,
    };
    try range.validate(object_len);

    const metadata_tail_offset: ?usize = if (metadata_offset >= tail_offset)
        @intCast(metadata_offset - tail_offset)
    else
        null;
    if (metadata_tail_offset) |offset| {
        const len: usize = @intCast(footer_metadata_len);
        if (offset > tail_bytes.len or len > tail_bytes.len - offset) return error.InvalidParquetFooter;
        if (offset + len > len_offset) return error.InvalidParquetFooter;
    }

    return .{
        .footer_metadata_len = footer_metadata_len,
        .footer_metadata_range = range,
        .metadata_tail_offset = metadata_tail_offset,
    };
}

pub fn planFooterMetadataRead(
    object: range_io.ObjectRef,
    tail_offset: u64,
    tail_bytes: []const u8,
) !range_io.RangeRead {
    try object.validate();
    const preflight = try parseFooterPreflight(object.byte_len, tail_offset, tail_bytes);
    const read = range_io.RangeRead{
        .object = object,
        .range = preflight.footer_metadata_range,
        .purpose = .parquet_footer,
    };
    try read.validate();
    return read;
}

test "parquet footer preflight finds metadata already in tail" {
    const metadata = [_]u8{ 'm', 'e', 't', 'a', 'd', 'a', 't', 'a' };
    const bytes = metadata ++ [_]u8{ @intCast(metadata.len), 0, 0, 0 } ++ [_]u8{ 'P', 'A', 'R', '1' };
    const preflight = try parseFooterPreflight(bytes.len, 0, &bytes);

    try std.testing.expectEqual(@as(u32, metadata.len), preflight.footer_metadata_len);
    try std.testing.expectEqual(@as(u64, 0), preflight.footer_metadata_range.offset);
    try std.testing.expectEqual(@as(u64, metadata.len), preflight.footer_metadata_range.len);
    try std.testing.expect(preflight.metadataAvailableInTail());
    try std.testing.expectEqualStrings(&metadata, preflight.metadataSlice(&bytes).?);
}

test "parquet footer preflight plans exact metadata read when tail is partial" {
    const object_len: u64 = 10_000;
    const footer_metadata_len: u32 = 1024;
    const tail = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 } ++ [_]u8{
        @intCast(footer_metadata_len & 0xff),
        @intCast((footer_metadata_len >> 8) & 0xff),
        @intCast((footer_metadata_len >> 16) & 0xff),
        @intCast((footer_metadata_len >> 24) & 0xff),
    } ++ [_]u8{ 'P', 'A', 'R', '1' };
    const tail_offset = object_len - tail.len;
    const preflight = try parseFooterPreflight(object_len, tail_offset, &tail);

    try std.testing.expect(!preflight.metadataAvailableInTail());
    try std.testing.expectEqual(@as(u64, object_len - trailer_len - footer_metadata_len), preflight.footer_metadata_range.offset);
    try std.testing.expectEqual(@as(u64, footer_metadata_len), preflight.footer_metadata_range.len);

    const object = range_io.ObjectRef{
        .bucket = "bucket",
        .key = "events/part-a.parquet",
        .byte_len = object_len,
        .version = .{ .etag = "etag-a" },
    };
    const read = try planFooterMetadataRead(object, tail_offset, &tail);
    try std.testing.expectEqual(range_io.RangePurpose.parquet_footer, read.purpose);
    try std.testing.expectEqual(preflight.footer_metadata_range.offset, read.range.offset);
    try std.testing.expectEqual(preflight.footer_metadata_range.len, read.range.len);
}

test "parquet footer preflight rejects invalid trailers" {
    const short = [_]u8{ 1, 2, 3, 4 };
    try std.testing.expectError(error.InvalidParquetFooter, parseFooterPreflight(short.len, 0, &short));

    const bad_magic = [_]u8{ 4, 0, 0, 0 } ++ [_]u8{ 'B', 'A', 'D', '!' };
    try std.testing.expectError(error.InvalidParquetFooterMagic, parseFooterPreflight(bad_magic.len, 0, &bad_magic));

    const overflow_len: u32 = 100;
    const overflow = [_]u8{
        @intCast(overflow_len & 0xff),
        @intCast((overflow_len >> 8) & 0xff),
        @intCast((overflow_len >> 16) & 0xff),
        @intCast((overflow_len >> 24) & 0xff),
    } ++ [_]u8{ 'P', 'A', 'R', '1' };
    try std.testing.expectError(error.InvalidParquetFooter, parseFooterPreflight(overflow.len, 0, &overflow));
}

test "parquet footer preflight rejects oversized metadata before its range is fetched" {
    const footer_metadata_len: u32 = @intCast(range_io.max_parquet_footer_metadata_bytes + 1);
    var tail = @as([trailer_len]u8, @splat(0));
    std.mem.writeInt(u32, tail[0..4], footer_metadata_len, .little);
    @memcpy(tail[4..8], parquet_magic);
    try std.testing.expectError(
        error.ParquetMetadataTooLarge,
        parseFooterPreflight(@as(u64, footer_metadata_len) + trailer_len, footer_metadata_len, &tail),
    );
}
