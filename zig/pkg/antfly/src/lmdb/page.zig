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

const std = @import("std");
const format = @import("format.zig");

pub const Error = error{
    TruncatedPage,
};

pub const Kind = enum {
    branch,
    leaf,
    overflow,
    meta,
    leaf2,
    subpage,
    unknown,
};

pub const View = struct {
    bytes: []const u8,

    pub fn init(bytes: []const u8) Error!View {
        if (bytes.len < format.page_header_size) return error.TruncatedPage;
        return .{ .bytes = bytes };
    }

    pub fn header(self: View) format.PageHeader {
        return format.readStruct(format.PageHeader, self.bytes[0..format.page_header_size]);
    }

    pub fn pgno(self: View) format.Pgno {
        return self.header().mp_pgno;
    }

    pub fn pad(self: View) u16 {
        return self.header().mp_pad;
    }

    pub fn flags(self: View) u16 {
        return self.header().mp_flags;
    }

    pub fn hasFlag(self: View, flag: u16) bool {
        return (self.flags() & flag) == flag;
    }

    pub fn lower(self: View) format.Indx {
        return self.header().mp_lower;
    }

    pub fn upper(self: View) format.Indx {
        return self.header().mp_upper;
    }

    pub fn overflowPageCount(self: View) u32 {
        const field_offset = @offsetOf(format.PageHeader, "mp_lower");
        return format.readNativeInt(u32, self.bytes[field_offset .. field_offset + @sizeOf(u32)]);
    }

    pub fn nodeCount(self: View) usize {
        return (self.lower() - @as(format.Indx, @intCast(format.page_header_size))) >> 1;
    }

    pub fn data(self: View) []const u8 {
        return self.bytes[format.page_header_size..];
    }

    pub fn kind(self: View) Kind {
        const page_flags = self.flags();
        if ((page_flags & format.PageFlags.meta) != 0) return .meta;
        if ((page_flags & format.PageFlags.overflow) != 0) return .overflow;
        if ((page_flags & format.PageFlags.branch) != 0) return .branch;
        if ((page_flags & format.PageFlags.leaf2) != 0) return .leaf2;
        if ((page_flags & format.PageFlags.leaf) != 0) return .leaf;
        if ((page_flags & format.PageFlags.subp) != 0) return .subpage;
        return .unknown;
    }
};

test "page view reads common header fields" {
    var page_bytes = std.mem.zeroes([64]u8);
    const hdr = format.PageHeader{
        .mp_pgno = 42,
        .mp_pad = 7,
        .mp_flags = format.PageFlags.leaf,
        .mp_lower = format.page_header_size + 6,
        .mp_upper = 60,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const view = try View.init(&page_bytes);
    try std.testing.expectEqual(@as(format.Pgno, 42), view.pgno());
    try std.testing.expectEqual(@as(u16, 7), view.pad());
    try std.testing.expectEqual(@as(u16, format.PageFlags.leaf), view.flags());
    try std.testing.expectEqual(@as(format.Indx, format.page_header_size + 6), view.lower());
    try std.testing.expectEqual(@as(format.Indx, 60), view.upper());
    try std.testing.expectEqual(@as(usize, 3), view.nodeCount());
    try std.testing.expectEqual(Kind.leaf, view.kind());
}

test "overflow pages read page count from lower/upper field union" {
    var page_bytes = std.mem.zeroes([64]u8);
    const hdr = format.PageHeader{
        .mp_pgno = 8,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.overflow,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const union_offset = @offsetOf(format.PageHeader, "mp_lower");
    format.writeNativeInt(u32, page_bytes[union_offset .. union_offset + @sizeOf(u32)], 5);

    const view = try View.init(&page_bytes);
    try std.testing.expectEqual(Kind.overflow, view.kind());
    try std.testing.expectEqual(@as(u32, 5), view.overflowPageCount());
}
