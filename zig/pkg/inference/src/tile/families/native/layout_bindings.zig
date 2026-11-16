// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Historical ABI bindings for the shared dense-row programs. No algorithms.
const Program = @import("antfly_tile").native_layout.Program;

pub const cuda = .{
    .window_pack = (@import("antfly_tile").native_layout.Windows{ .direction = .pack }).native(.{ .integer = "unsigned int", .prefix = "", .index = "idx", .input = "src", .output = "dst" }),
    .window_unpack = (@import("antfly_tile").native_layout.Windows{ .direction = .unpack }).native(.{ .integer = "unsigned int", .prefix = "", .index = "idx", .input = "src", .output = "dst" }),
    .transpose = @import("antfly_tile").native_layout.cuda_transpose.native(.{ .integer = "unsigned int", .shared = "__shared__", .barrier = "__syncthreads();", .rows = "rows", .cols = "cols", .x = "threadIdx.x & 31u", .y = "threadIdx.x >> 5u", .group_x = "blockIdx.x % tile_cols", .group_y = "blockIdx.x / tile_cols" }),
    .take_rows = (Program{ .operation = .take }).native(.{ .integer = "unsigned int", .index = "idx", .output = "dst", .rows = "rows", .cols = "dim" }),
    .slice_rows = (Program{ .operation = .slice }).native(.{ .integer = "unsigned int", .index = "idx", .output = "dst", .rows = "rows", .cols = "out_cols" }),
    .repeat_rows = (Program{ .operation = .repeat }).native(.{ .integer = "unsigned int", .index = "idx", .input = "src", .output = "dst", .rows = "rows", .cols = "dim" }),
    .concat_rows = (Program{ .operation = .concat }).native(.{ .integer = "unsigned int", .index = "idx", .input = "a", .output = "dst", .rows = "total", .cols = "out_dim", .source_cols = "dim_a", .branch_concat = true }),
};
pub const metal = .{
    .transpose = @import("antfly_tile").native_layout.metal_transpose.native(.{ .integer = "uint", .shared = "threadgroup", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .rows = "p.rows", .cols = "p.cols", .x = "tid.x", .y = "tid.y", .group_x = "tg.x", .group_y = "tg.y", .base = "tg.z * p.rows * p.cols" }),
    .slice_rows = (Program{ .operation = .slice }).native(.{ .integer = "uint", .index = "gid", .input = "src", .output = "dst", .rows = "p.rows", .cols = "p.out_cols", .source_cols = "p.cols", .start = "p.start", .row_bounds = true }),
    .concat_rows = (Program{ .operation = .concat }).native(.{ .integer = "uint", .index = "gid", .input = "a", .rows = "p.rows", .cols = "out_dim", .source_cols = "p.dim_a", .second_cols = "p.dim_b" }),
};
pub const cuda_sequence = .{
    .gru = @import("antfly_tile").native_math.Gru.native(@import("antfly_tile").native_math.cuda, "unsigned int", "idx", "rows", "dim", "dst", "add_residual != 0u"),
    .first_match_rows = (Program{ .operation = .first_match }).native(.{ .integer = "unsigned int", .index = "idx", .input = "hidden", .output = "dst", .rows = "(batch * num_words)", .cols = "hidden_size" }),
};
pub const metal_sequence = .{
    .moments_tree = @import("row_statistics.zig").metal.moments_tree,
    .gru = @import("antfly_tile").native_math.Gru.native(@import("antfly_tile").native_math.metal, "uint", "gid", "p.rows", "p.dim", "output", "p.add_residual != 0u"),
    .first_match_rows = (Program{ .operation = .first_match }).native(.{ .integer = "uint", .index = "gid", .input = "hidden", .rows = "(p.batch * p.num_words)", .cols = "p.hidden_size", .sequence = "p.seq_len", .words = "p.num_words", .mask_type = "float" }),
};

pub const metal_abi = .{
    .window_pack = @import("../../native_program.zig").metal((@import("antfly_tile").native_layout.Windows{ .direction = .pack }).native(.{ .integer = "uint", .prefix = "p.", .index = "gid", .input = "input", .output = "output", .supplied_geometry = true })),
    .window_unpack = @import("../../native_program.zig").metal((@import("antfly_tile").native_layout.Windows{ .direction = .unpack }).native(.{ .integer = "uint", .prefix = "p.", .index = "gid", .input = "input", .output = "output", .supplied_geometry = true })),
};
