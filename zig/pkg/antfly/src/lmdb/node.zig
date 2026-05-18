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
const page = @import("page.zig");

pub const Error = error{
    InvalidNodeIndex,
    InvalidNodeOffset,
    TruncatedNode,
};

pub const Header = extern struct {
    mn_lo: u16,
    mn_hi: u16,
    mn_flags: u16,
    mn_ksize: u16,
};

pub const header_size: usize = @sizeOf(Header);

pub const View = struct {
    page_bytes: []const u8,
    offset: usize,
    header: Header,

    pub fn init(page_bytes: []const u8, offset: usize) Error!View {
        if (offset + header_size > page_bytes.len) return error.TruncatedNode;

        const header = format.readStruct(Header, page_bytes[offset..][0..header_size]);
        return .{
            .page_bytes = page_bytes,
            .offset = offset,
            .header = header,
        };
    }

    pub fn fromPage(page_view: page.View, index: usize) Error!View {
        const node_count = page_view.nodeCount();
        if (index >= node_count) return error.InvalidNodeIndex;

        const ptr_offset = format.page_header_size + index * @sizeOf(format.Indx);
        if (ptr_offset + @sizeOf(format.Indx) > page_view.bytes.len) return error.InvalidNodeOffset;

        const node_offset = format.readNativeInt(format.Indx, page_view.bytes[ptr_offset..][0..@sizeOf(format.Indx)]);
        if (node_offset < format.page_header_size or node_offset >= page_view.bytes.len) return error.InvalidNodeOffset;

        const view = try init(page_view.bytes, node_offset);
        const required_len = switch (page_view.kind()) {
            .branch => header_size + view.key().len,
            .leaf => header_size + view.key().len + view.storedValueLen(),
            else => return error.TruncatedNode,
        };
        if (node_offset + required_len > page_view.bytes.len) return error.TruncatedNode;
        return view;
    }

    pub fn flags(self: View) u16 {
        return self.header.mn_flags;
    }

    pub fn keySize(self: View) usize {
        return self.header.mn_ksize;
    }

    pub fn key(self: View) []const u8 {
        const start = self.offset + header_size;
        return self.page_bytes[start .. start + self.keySize()];
    }

    pub fn dataSize(self: View) usize {
        return @as(usize, self.header.mn_lo) | (@as(usize, self.header.mn_hi) << 16);
    }

    pub fn storedValueLen(self: View) usize {
        if (self.isBigData()) return @sizeOf(format.Pgno);
        return self.dataSize();
    }

    pub fn inlineValue(self: View) []const u8 {
        const start = self.offset + header_size + self.keySize();
        return self.page_bytes[start .. start + self.storedValueLen()];
    }

    pub fn isBigData(self: View) bool {
        return (self.flags() & format.NodeFlags.bigdata) != 0;
    }

    pub fn isSubdata(self: View) bool {
        return (self.flags() & format.NodeFlags.subdata) != 0;
    }

    pub fn isDupdata(self: View) bool {
        return (self.flags() & format.NodeFlags.dupdata) != 0;
    }

    pub fn branchPgno(self: View) format.Pgno {
        return @as(format.Pgno, self.header.mn_lo) |
            (@as(format.Pgno, self.header.mn_hi) << 16) |
            (@as(format.Pgno, self.header.mn_flags) << 32);
    }
};

test "node view reads leaf node fields" {
    var bytes = std.mem.zeroes([64]u8);
    const hdr = Header{
        .mn_lo = 3,
        .mn_hi = 0,
        .mn_flags = 0,
        .mn_ksize = 2,
    };
    format.writeStruct(Header, bytes[10..][0..header_size], hdr);
    @memcpy(bytes[10 + header_size .. 10 + header_size + 2], "id");
    @memcpy(bytes[10 + header_size + 2 .. 10 + header_size + 5], "cat");

    const view = try View.init(&bytes, 10);
    try std.testing.expectEqualStrings("id", view.key());
    try std.testing.expectEqual(@as(usize, 3), view.dataSize());
    try std.testing.expectEqualStrings("cat", view.inlineValue());
}

test "branch pgno uses header fields as child page number" {
    const hdr = Header{
        .mn_lo = 0x5678,
        .mn_hi = 0x1234,
        .mn_flags = 0,
        .mn_ksize = 0,
    };
    const bytes = std.mem.zeroes([32]u8);
    _ = bytes;
    const view = View{
        .page_bytes = &.{},
        .offset = 0,
        .header = hdr,
    };
    try std.testing.expectEqual(@as(format.Pgno, 0x12345678), view.branchPgno());
}
