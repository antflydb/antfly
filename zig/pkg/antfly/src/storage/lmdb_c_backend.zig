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

const c = @import("lmdb_c_api.zig").Bindings;

pub const ValuePair = struct {
    key: []const u8,
    value: []const u8,
};

fn toVal(data: []const u8) c.MDB_val {
    return .{ .mv_size = data.len, .mv_data = @ptrCast(@constCast(data.ptr)) };
}

fn fromVal(val: c.MDB_val) []const u8 {
    if (val.mv_data == null or val.mv_size == 0) return &.{};
    const ptr: [*]const u8 = @ptrCast(val.mv_data.?);
    return ptr[0..val.mv_size];
}

fn fromMutableVal(val: c.MDB_val) []u8 {
    if (val.mv_data == null or val.mv_size == 0) return &.{};
    const ptr: [*]u8 = @ptrCast(val.mv_data.?);
    return ptr[0..val.mv_size];
}

pub fn openEnvironment(path: [*:0]const u8, opts: anytype, check: anytype) !*c.MDB_env {
    var env: ?*c.MDB_env = null;
    try check(c.mdb_env_create(&env));
    const handle = env.?;
    errdefer c.mdb_env_close(handle);

    if (opts.max_dbs > 0) try check(c.mdb_env_set_maxdbs(handle, opts.max_dbs));
    try check(c.mdb_env_set_maxreaders(handle, opts.max_readers));
    try check(c.mdb_env_set_mapsize(handle, opts.map_size));

    var flags: u32 = 0;
    if (opts.fixed_map) flags |= c.MDB_FIXEDMAP;
    if (opts.no_subdir) flags |= c.MDB_NOSUBDIR;
    if (opts.read_only) flags |= c.MDB_RDONLY;
    if (opts.no_sync) flags |= c.MDB_NOSYNC;
    if (opts.no_meta_sync) flags |= c.MDB_NOMETASYNC;
    if (opts.no_tls) flags |= c.MDB_NOTLS;
    if (opts.write_map) flags |= c.MDB_WRITEMAP;
    if (opts.map_async) flags |= c.MDB_MAPASYNC;
    if (opts.no_lock) flags |= c.MDB_NOLOCK;
    if (opts.no_read_ahead) flags |= c.MDB_NORDAHEAD;
    if (opts.no_mem_init) flags |= c.MDB_NOMEMINIT;

    try check(c.mdb_env_open(handle, path, flags, @intCast(opts.mode)));
    return handle;
}

pub fn syncEnvironment(env: *c.MDB_env, force: bool, check: anytype) !void {
    try check(c.mdb_env_sync(env, if (force) 1 else 0));
}

pub fn closeEnvironment(env: *c.MDB_env) void {
    c.mdb_env_close(env);
}

pub fn setMapSize(env: *c.MDB_env, size: usize, check: anytype) !void {
    try check(c.mdb_env_set_mapsize(env, size));
}

pub fn beginTransaction(env: *c.MDB_env, opts: anytype, check: anytype) !*c.MDB_txn {
    var txn: ?*c.MDB_txn = null;
    const flags: u32 = if (opts.read_only) c.MDB_RDONLY else 0;
    try check(c.mdb_txn_begin(env, null, flags, &txn));
    return txn.?;
}

pub fn beginChildTransaction(txn: *c.MDB_txn, check: anytype) !*c.MDB_txn {
    var child: ?*c.MDB_txn = null;
    try check(c.mdb_txn_begin(c.mdb_txn_env(txn), txn, 0, &child));
    return child.?;
}

pub fn commitTransaction(txn: *c.MDB_txn, check: anytype) !void {
    try check(c.mdb_txn_commit(txn));
}

pub fn abortTransaction(txn: *c.MDB_txn) void {
    c.mdb_txn_abort(txn);
}

pub fn openDb(txn: *c.MDB_txn, name: ?[*:0]const u8, opts: anytype, check: anytype) !c.MDB_dbi {
    var dbi: c.MDB_dbi = 0;
    var flags: u32 = 0;
    if (opts.create) flags |= c.MDB_CREATE;
    if (opts.reverse_key) flags |= c.MDB_REVERSEKEY;
    if (opts.integer_key) flags |= c.MDB_INTEGERKEY;
    if (opts.dup_sort) flags |= c.MDB_DUPSORT;
    if (opts.dup_fixed) flags |= c.MDB_DUPFIXED;
    if (opts.integer_dup) flags |= c.MDB_INTEGERDUP;
    if (opts.reverse_dup) flags |= c.MDB_REVERSEDUP;
    try check(c.mdb_dbi_open(txn, name, flags, &dbi));
    return dbi;
}

pub fn openCursor(txn: *c.MDB_txn, dbi: c.MDB_dbi, check: anytype) !*c.MDB_cursor {
    var cursor: ?*c.MDB_cursor = null;
    try check(c.mdb_cursor_open(txn, dbi, &cursor));
    return cursor.?;
}

pub fn get(txn: *c.MDB_txn, dbi: c.MDB_dbi, key: []const u8, check: anytype) ![]u8 {
    var k = toVal(key);
    var v: c.MDB_val = undefined;
    try check(c.mdb_get(txn, dbi, &k, &v));
    return fromMutableVal(v);
}

pub fn put(txn: *c.MDB_txn, dbi: c.MDB_dbi, key: []const u8, value: []const u8, opts: anytype, check: anytype) !void {
    var k = toVal(key);
    var v = toVal(value);
    var flags: u32 = 0;
    if (opts.no_overwrite) flags |= c.MDB_NOOVERWRITE;
    if (opts.no_dup_data) flags |= c.MDB_NODUPDATA;
    if (opts.append) flags |= c.MDB_APPEND;
    if (opts.append_dup) flags |= c.MDB_APPENDDUP;
    try check(c.mdb_put(txn, dbi, &k, &v, flags));
}

pub fn reserve(txn: *c.MDB_txn, dbi: c.MDB_dbi, key: []const u8, size: usize, opts: anytype, check: anytype) ![]u8 {
    var k = toVal(key);
    var v = c.MDB_val{ .mv_size = size, .mv_data = null };
    var flags: u32 = c.MDB_RESERVE;
    if (opts.no_overwrite) flags |= c.MDB_NOOVERWRITE;
    if (opts.append) flags |= c.MDB_APPEND;
    try check(c.mdb_put(txn, dbi, &k, &v, flags));
    return fromMutableVal(v);
}

pub fn delete(txn: *c.MDB_txn, dbi: c.MDB_dbi, key: []const u8, check: anytype) !void {
    var k = toVal(key);
    try check(c.mdb_del(txn, dbi, &k, null));
}

pub fn deleteValue(txn: *c.MDB_txn, dbi: c.MDB_dbi, key: []const u8, value: []const u8, check: anytype) !void {
    var k = toVal(key);
    var v = toVal(value);
    try check(c.mdb_del(txn, dbi, &k, &v));
}

pub fn closeCursor(cursor: *c.MDB_cursor) void {
    c.mdb_cursor_close(cursor);
}

pub fn cursorGet(cursor: *c.MDB_cursor, op: u32, check: anytype) !ValuePair {
    var k: c.MDB_val = undefined;
    var v: c.MDB_val = undefined;
    try check(c.mdb_cursor_get(cursor, &k, &v, op));
    return .{ .key = fromVal(k), .value = fromVal(v) };
}

pub fn cursorGetWithKey(cursor: *c.MDB_cursor, key: []const u8, op: u32, check: anytype) !ValuePair {
    var k = toVal(key);
    var v: c.MDB_val = undefined;
    try check(c.mdb_cursor_get(cursor, &k, &v, op));
    return .{ .key = fromVal(k), .value = fromVal(v) };
}

pub fn cursorGetWithKeyValue(cursor: *c.MDB_cursor, key: []const u8, value: []const u8, op: u32, check: anytype) !ValuePair {
    var k = toVal(key);
    var v = toVal(value);
    try check(c.mdb_cursor_get(cursor, &k, &v, op));
    return .{ .key = fromVal(k), .value = fromVal(v) };
}

pub fn cursorPut(cursor: *c.MDB_cursor, key: []const u8, value: []const u8, opts: anytype, check: anytype) !void {
    var k = toVal(key);
    var v = toVal(value);
    var flags: u32 = 0;
    if (opts.no_overwrite) flags |= c.MDB_NOOVERWRITE;
    if (opts.no_dup_data) flags |= c.MDB_NODUPDATA;
    if (opts.append) flags |= c.MDB_APPEND;
    if (opts.append_dup) flags |= c.MDB_APPENDDUP;
    try check(c.mdb_cursor_put(cursor, &k, &v, flags));
}

pub fn cursorReserve(cursor: *c.MDB_cursor, key: []const u8, size: usize, opts: anytype, check: anytype) ![]u8 {
    var k = toVal(key);
    var v = c.MDB_val{ .mv_size = size, .mv_data = null };
    var flags: u32 = c.MDB_RESERVE;
    if (opts.no_overwrite) flags |= c.MDB_NOOVERWRITE;
    if (opts.append) flags |= c.MDB_APPEND;
    try check(c.mdb_cursor_put(cursor, &k, &v, flags));
    return fromMutableVal(v);
}

pub fn deleteCurrent(cursor: *c.MDB_cursor, check: anytype) !void {
    try check(c.mdb_cursor_del(cursor, 0));
}
