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
    TruncatedMetaPage,
    NotMetaPage,
    InvalidMagic,
    InvalidVersion,
    InvalidPageSize,
};

pub const Parsed = struct {
    header: format.PageHeader,
    meta: format.Meta,

    pub fn pageSize(self: Parsed) u32 {
        return self.meta.mm_dbs[format.free_dbi].md_pad;
    }

    pub fn persistentFlags(self: Parsed) u16 {
        return self.meta.mm_dbs[format.free_dbi].md_flags;
    }

    pub fn freeDb(self: Parsed) format.Db {
        return self.meta.mm_dbs[format.free_dbi];
    }

    pub fn mainDb(self: Parsed) format.Db {
        return self.meta.mm_dbs[format.main_dbi];
    }

    pub fn txnid(self: Parsed) format.Txnid {
        return self.meta.mm_txnid;
    }
};

pub fn parse(bytes: []const u8) Error!Parsed {
    const view = page.View.init(bytes) catch return error.TruncatedMetaPage;
    if (view.kind() != .meta) return error.NotMetaPage;

    const body = view.data();
    if (body.len < format.meta_body_size) return error.TruncatedMetaPage;

    const parsed = Parsed{
        .header = view.header(),
        .meta = format.readStruct(format.Meta, body[0..format.meta_body_size]),
    };

    if (parsed.meta.mm_magic != format.mdb_magic) return error.InvalidMagic;
    if (parsed.meta.mm_version != format.mdb_data_version) return error.InvalidVersion;

    const page_size = parsed.pageSize();
    if (page_size == 0 or !std.math.isPowerOfTwo(page_size) or page_size > format.max_pagesize) {
        return error.InvalidPageSize;
    }

    return parsed;
}

pub fn newer(a: Parsed, b: Parsed) Parsed {
    if (a.txnid() >= b.txnid()) return a;
    return b;
}

test "parse valid meta page" {
    var page_bytes = std.mem.zeroes([4096]u8);
    const hdr = format.PageHeader{
        .mp_pgno = 1,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, page_bytes[0..format.page_header_size], hdr);

    const free_db = format.Db{
        .md_pad = 4096,
        .md_flags = 0,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 1,
        .md_branch_pages = 2,
        .md_leaf_pages = 3,
        .md_overflow_pages = 4,
        .md_entries = 99,
        .md_root = 7,
    };
    const meta_value = format.Meta{
        .mm_magic = format.mdb_magic,
        .mm_version = format.mdb_data_version,
        .mm_address = null,
        .mm_mapsize = 1 << 20,
        .mm_dbs = .{ free_db, main_db },
        .mm_last_pg = 12,
        .mm_txnid = 33,
    };
    format.writeStruct(format.Meta, page_bytes[format.page_header_size..][0..format.meta_body_size], meta_value);

    const parsed = try parse(&page_bytes);
    try std.testing.expectEqual(@as(format.Pgno, 1), parsed.header.mp_pgno);
    try std.testing.expectEqual(@as(u32, 4096), parsed.pageSize());
    try std.testing.expectEqual(@as(usize, 1 << 20), parsed.meta.mm_mapsize);
    try std.testing.expectEqual(@as(format.Pgno, 7), parsed.mainDb().md_root);
    try std.testing.expectEqual(@as(format.Txnid, 33), parsed.txnid());
}

test "reject non-meta page and bad magic" {
    var non_meta = std.mem.zeroes([256]u8);
    const leaf_hdr = format.PageHeader{
        .mp_pgno = 2,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.leaf,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, non_meta[0..format.page_header_size], leaf_hdr);
    try std.testing.expectError(error.NotMetaPage, parse(&non_meta));

    var bad_magic = std.mem.zeroes([4096]u8);
    const meta_hdr = format.PageHeader{
        .mp_pgno = 0,
        .mp_pad = 0,
        .mp_flags = format.PageFlags.meta,
        .mp_lower = 0,
        .mp_upper = 0,
    };
    format.writeStruct(format.PageHeader, bad_magic[0..format.page_header_size], meta_hdr);
    const meta_value = format.Meta{
        .mm_magic = 0,
        .mm_version = format.mdb_data_version,
        .mm_address = null,
        .mm_mapsize = 4096 * format.num_metas,
        .mm_dbs = .{
            .{
                .md_pad = 4096,
                .md_flags = 0,
                .md_depth = 0,
                .md_branch_pages = 0,
                .md_leaf_pages = 0,
                .md_overflow_pages = 0,
                .md_entries = 0,
                .md_root = format.invalid_pgno,
            },
            .{
                .md_pad = 0,
                .md_flags = 0,
                .md_depth = 0,
                .md_branch_pages = 0,
                .md_leaf_pages = 0,
                .md_overflow_pages = 0,
                .md_entries = 0,
                .md_root = format.invalid_pgno,
            },
        },
        .mm_last_pg = format.num_metas - 1,
        .mm_txnid = 1,
    };
    format.writeStruct(format.Meta, bad_magic[format.page_header_size..][0..format.meta_body_size], meta_value);
    try std.testing.expectError(error.InvalidMagic, parse(&bad_magic));
}

test "newer picks the highest txnid" {
    const a = Parsed{
        .header = undefined,
        .meta = .{
            .mm_magic = format.mdb_magic,
            .mm_version = format.mdb_data_version,
            .mm_address = null,
            .mm_mapsize = 0,
            .mm_dbs = undefined,
            .mm_last_pg = 0,
            .mm_txnid = 8,
        },
    };
    const b = Parsed{
        .header = undefined,
        .meta = .{
            .mm_magic = format.mdb_magic,
            .mm_version = format.mdb_data_version,
            .mm_address = null,
            .mm_mapsize = 0,
            .mm_dbs = undefined,
            .mm_last_pg = 0,
            .mm_txnid = 11,
        },
    };

    try std.testing.expectEqual(@as(format.Txnid, 11), newer(a, b).txnid());
    try std.testing.expectEqual(@as(format.Txnid, 11), newer(b, a).txnid());
}
