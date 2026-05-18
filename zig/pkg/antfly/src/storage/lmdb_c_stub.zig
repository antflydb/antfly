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

pub const MDB_env = opaque {};
pub const MDB_txn = opaque {};
pub const MDB_cursor = opaque {};
pub const MDB_dbi = u32;

pub const MDB_val = extern struct {
    mv_size: usize,
    mv_data: ?*anyopaque,
};

pub const MDB_FIXEDMAP: u32 = 0x01;
pub const MDB_NOSUBDIR: u32 = 0x4000;
pub const MDB_NOSYNC: u32 = 0x10000;
pub const MDB_RDONLY: u32 = 0x20000;
pub const MDB_NOMETASYNC: u32 = 0x40000;
pub const MDB_WRITEMAP: u32 = 0x80000;
pub const MDB_MAPASYNC: u32 = 0x100000;
pub const MDB_NOTLS: u32 = 0x200000;
pub const MDB_NOLOCK: u32 = 0x400000;
pub const MDB_NORDAHEAD: u32 = 0x800000;
pub const MDB_NOMEMINIT: u32 = 0x1000000;

pub const MDB_REVERSEKEY: u32 = 0x02;
pub const MDB_DUPSORT: u32 = 0x04;
pub const MDB_INTEGERKEY: u32 = 0x08;
pub const MDB_DUPFIXED: u32 = 0x10;
pub const MDB_INTEGERDUP: u32 = 0x20;
pub const MDB_REVERSEDUP: u32 = 0x40;
pub const MDB_CREATE: u32 = 0x40000;

pub const MDB_NOOVERWRITE: u32 = 0x10;
pub const MDB_NODUPDATA: u32 = 0x20;
pub const MDB_RESERVE: u32 = 0x10000;
pub const MDB_APPEND: u32 = 0x20000;
pub const MDB_APPENDDUP: u32 = 0x40000;

pub const MDB_KEYEXIST: i32 = -30799;
pub const MDB_NOTFOUND: i32 = -30798;
pub const MDB_PAGE_NOTFOUND: i32 = -30797;
pub const MDB_CORRUPTED: i32 = -30796;
pub const MDB_PANIC: i32 = -30795;
pub const MDB_VERSION_MISMATCH: i32 = -30794;
pub const MDB_INVALID: i32 = -30793;
pub const MDB_MAP_FULL: i32 = -30792;
pub const MDB_DBS_FULL: i32 = -30791;
pub const MDB_READERS_FULL: i32 = -30790;
pub const MDB_TLS_FULL: i32 = -30789;
pub const MDB_TXN_FULL: i32 = -30788;
pub const MDB_CURSOR_FULL: i32 = -30787;
pub const MDB_PAGE_FULL: i32 = -30786;
pub const MDB_MAP_RESIZED: i32 = -30785;
pub const MDB_INCOMPATIBLE: i32 = -30784;
pub const MDB_BAD_RSLOT: i32 = -30783;
pub const MDB_BAD_TXN: i32 = -30782;
pub const MDB_BAD_VALSIZE: i32 = -30781;
pub const MDB_BAD_DBI: i32 = -30780;

pub const MDB_FIRST: u32 = 0;
pub const MDB_FIRST_DUP: u32 = 1;
pub const MDB_GET_BOTH: u32 = 2;
pub const MDB_GET_BOTH_RANGE: u32 = 3;
pub const MDB_GET_CURRENT: u32 = 4;
pub const MDB_LAST: u32 = 6;
pub const MDB_LAST_DUP: u32 = 7;
pub const MDB_NEXT: u32 = 8;
pub const MDB_NEXT_DUP: u32 = 9;
pub const MDB_NEXT_NODUP: u32 = 11;
pub const MDB_PREV: u32 = 12;
pub const MDB_PREV_DUP: u32 = 13;
pub const MDB_PREV_NODUP: u32 = 14;
pub const MDB_SET: u32 = 15;
pub const MDB_SET_KEY: u32 = 16;
pub const MDB_SET_RANGE: u32 = 17;

pub fn mdb_env_create(_: *?*MDB_env) i32 {
    unreachable;
}

pub fn mdb_env_close(_: *MDB_env) void {
    unreachable;
}

pub fn mdb_env_set_maxdbs(_: *MDB_env, _: u32) i32 {
    unreachable;
}

pub fn mdb_env_set_maxreaders(_: *MDB_env, _: u32) i32 {
    unreachable;
}

pub fn mdb_env_set_mapsize(_: *MDB_env, _: usize) i32 {
    unreachable;
}

pub fn mdb_env_open(_: *MDB_env, _: [*:0]const u8, _: u32, _: u32) i32 {
    unreachable;
}

pub fn mdb_env_sync(_: *MDB_env, _: i32) i32 {
    unreachable;
}

pub fn mdb_txn_begin(_: *MDB_env, _: ?*MDB_txn, _: u32, _: *?*MDB_txn) i32 {
    unreachable;
}

pub fn mdb_txn_commit(_: *MDB_txn) i32 {
    unreachable;
}

pub fn mdb_txn_abort(_: *MDB_txn) void {
    unreachable;
}

pub fn mdb_txn_env(_: *MDB_txn) *MDB_env {
    unreachable;
}

pub fn mdb_dbi_open(_: *MDB_txn, _: ?[*:0]const u8, _: u32, _: *MDB_dbi) i32 {
    unreachable;
}

pub fn mdb_get(_: *MDB_txn, _: MDB_dbi, _: *MDB_val, _: *MDB_val) i32 {
    unreachable;
}

pub fn mdb_put(_: *MDB_txn, _: MDB_dbi, _: *MDB_val, _: *MDB_val, _: u32) i32 {
    unreachable;
}

pub fn mdb_del(_: *MDB_txn, _: MDB_dbi, _: *MDB_val, _: ?*MDB_val) i32 {
    unreachable;
}

pub fn mdb_cursor_open(_: *MDB_txn, _: MDB_dbi, _: *?*MDB_cursor) i32 {
    unreachable;
}

pub fn mdb_cursor_close(_: *MDB_cursor) void {
    unreachable;
}

pub fn mdb_cursor_get(_: *MDB_cursor, _: *MDB_val, _: *MDB_val, _: u32) i32 {
    unreachable;
}

pub fn mdb_cursor_put(_: *MDB_cursor, _: *MDB_val, _: *MDB_val, _: u32) i32 {
    unreachable;
}

pub fn mdb_cursor_del(_: *MDB_cursor, _: u32) i32 {
    unreachable;
}
