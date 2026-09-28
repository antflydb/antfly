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
const build_options = @import("build_options");

pub const format = @import("format.zig");
pub const page = @import("page.zig");
pub const meta = @import("meta.zig");
pub const env = @import("env.zig");
pub const node = @import("node.zig");
pub const dupdata = @import("dupdata.zig");
pub const readers = @import("readers.zig");
pub const writer_lock = @import("writer_lock.zig");
pub const mutate_leaf = @import("mutate_leaf.zig");
pub const rebalance_branch = @import("rebalance_branch.zig");
pub const free_db = @import("free_db.zig");
pub const txn_support = @import("txn_support.zig");
pub const read_support = @import("read_support.zig");
pub const write_state = @import("write_state.zig");
pub const write_mutation_support = @import("write_mutation_support.zig");
pub const dupsort_write_support = @import("dupsort_write_support.zig");
pub const write_page_state_support = @import("write_page_state_support.zig");
pub const write_path_support = @import("write_path_support.zig");
pub const commit_support = @import("commit_support.zig");
pub const materialize_support = @import("materialize_support.zig");
pub const prepare_commit_support = @import("prepare_commit_support.zig");
pub const split_support = @import("split_support.zig");
pub const txn = @import("txn.zig");
pub const tree = @import("tree.zig");
pub const cursor = @import("cursor.zig");
pub const sim = @import("sim.zig");

pub const Backend = enum {
    c,
    zig,
};

pub const selected_backend_name = build_options.lmdb_backend;

pub const selected_backend: Backend =
    std.meta.stringToEnum(Backend, selected_backend_name) orelse
    @compileError("invalid build option for lmdb_backend");

pub const is_c_backend = selected_backend == .c;
pub const is_zig_backend = selected_backend == .zig;
pub const storage_sim_soak = build_options.storage_sim_soak;

test "lmdb backend build option is valid" {
    try std.testing.expect(is_c_backend or is_zig_backend);
}

test {
    _ = format;
    _ = page;
    _ = meta;
    _ = env;
    _ = node;
    _ = dupdata;
    _ = readers;
    _ = writer_lock;
    _ = mutate_leaf;
    _ = rebalance_branch;
    _ = free_db;
    _ = txn_support;
    _ = read_support;
    _ = write_state;
    _ = write_mutation_support;
    _ = dupsort_write_support;
    _ = write_page_state_support;
    _ = write_path_support;
    _ = commit_support;
    _ = materialize_support;
    _ = prepare_commit_support;
    _ = split_support;
    _ = txn;
    _ = tree;
    _ = cursor;
    _ = sim;
}
