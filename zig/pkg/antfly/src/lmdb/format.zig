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

const builtin = @import("builtin");
const std = @import("std");

pub const native_endian = builtin.cpu.arch.endian();

pub const Pgno = usize;
pub const Txnid = usize;
pub const Indx = u16;

pub const free_dbi: usize = 0;
pub const main_dbi: usize = 1;
pub const core_dbs: usize = 2;
pub const num_metas: usize = 2;

pub const invalid_pgno: Pgno = std.math.maxInt(Pgno);

pub const max_pagesize: usize = 0x8000;
pub const mdb_magic: u32 = 0xBEEFC0DE;
pub const mdb_data_version: u32 = 1;

pub const PageFlags = struct {
    pub const branch: u16 = 0x01;
    pub const leaf: u16 = 0x02;
    pub const overflow: u16 = 0x04;
    pub const meta: u16 = 0x08;
    pub const dirty: u16 = 0x10;
    pub const leaf2: u16 = 0x20;
    pub const subp: u16 = 0x40;
    pub const loose: u16 = 0x4000;
    pub const keep: u16 = 0x8000;
};

pub const NodeFlags = struct {
    pub const bigdata: u16 = 0x01;
    pub const subdata: u16 = 0x02;
    pub const dupdata: u16 = 0x04;
};

pub const DbFlags = struct {
    pub const reverse_key: u16 = 0x02;
    pub const dup_sort: u16 = 0x04;
    pub const integer_key: u16 = 0x08;
    pub const dup_fixed: u16 = 0x10;
    pub const integer_dup: u16 = 0x20;
    pub const reverse_dup: u16 = 0x40;
};

pub fn compareDbKeys(db_flags: u16, left: []const u8, right: []const u8) std.math.Order {
    if ((db_flags & DbFlags.integer_key) != 0) return compareInteger(left, right);
    if ((db_flags & DbFlags.reverse_key) != 0) return compareReverse(left, right);
    return std.mem.order(u8, left, right);
}

pub const Db = extern struct {
    md_pad: u32,
    md_flags: u16,
    md_depth: u16,
    md_branch_pages: Pgno,
    md_leaf_pages: Pgno,
    md_overflow_pages: Pgno,
    md_entries: usize,
    md_root: Pgno,
};

pub const Meta = extern struct {
    mm_magic: u32,
    mm_version: u32,
    mm_address: ?*anyopaque,
    mm_mapsize: usize,
    mm_dbs: [core_dbs]Db,
    mm_last_pg: Pgno,
    mm_txnid: Txnid,
};

pub const PageHeader = extern struct {
    mp_pgno: Pgno,
    mp_pad: u16,
    mp_flags: u16,
    mp_lower: Indx,
    mp_upper: Indx,
};

pub const page_header_size: usize = @sizeOf(PageHeader);
pub const meta_body_size: usize = @sizeOf(Meta);

pub fn readNativeInt(comptime T: type, bytes: []const u8) T {
    return std.mem.readInt(T, bytes[0..@sizeOf(T)], native_endian);
}

pub fn writeNativeInt(comptime T: type, out: []u8, value: T) void {
    std.mem.writeInt(T, out[0..@sizeOf(T)], value, native_endian);
}

pub fn readStruct(comptime T: type, bytes: []const u8) T {
    var value: T = undefined;
    @memcpy(std.mem.asBytes(&value), bytes[0..@sizeOf(T)]);
    return value;
}

pub fn writeStruct(comptime T: type, out: []u8, value: T) void {
    @memcpy(out[0..@sizeOf(T)], std.mem.asBytes(&value));
}

fn compareInteger(left: []const u8, right: []const u8) std.math.Order {
    if (left.len != right.len) return std.math.order(left.len, right.len);
    return switch (left.len) {
        @sizeOf(u32) => std.math.order(readNativeInt(u32, left), readNativeInt(u32, right)),
        @sizeOf(u64) => std.math.order(readNativeInt(u64, left), readNativeInt(u64, right)),
        else => std.mem.order(u8, left, right),
    };
}

fn compareReverse(left: []const u8, right: []const u8) std.math.Order {
    var i: usize = 0;
    const common = @min(left.len, right.len);
    while (i < common) : (i += 1) {
        const lb = left[left.len - 1 - i];
        const rb = right[right.len - 1 - i];
        if (lb < rb) return .lt;
        if (lb > rb) return .gt;
    }
    return std.math.order(left.len, right.len);
}

test "layout matches expected 64-bit LMDB sizes" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(Pgno));
    try std.testing.expectEqual(@as(usize, 48), @sizeOf(Db));
    try std.testing.expectEqual(@as(usize, 136), @sizeOf(Meta));
    try std.testing.expectEqual(@as(usize, 16), page_header_size);
}
