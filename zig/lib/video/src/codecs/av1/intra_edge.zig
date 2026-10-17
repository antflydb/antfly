// Copyright © 2018-2019, VideoLAN and dav1d authors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-2-Clause
// /*
//  * Copyright © 2018-2023, VideoLAN and dav1d authors
//  * Copyright © 2018-2023, Two Orioles, LLC
//  * All rights reserved.
//  *
//  * Redistribution and use in source and binary forms, with or without
//  * modification, are permitted provided that the following conditions are met:
//  *
//  * 1. Redistributions of source code must retain the above copyright notice, this
//  *    list of conditions and the following disclaimer.
//  *
//  * 2. Redistributions in binary form must reproduce the above copyright notice,
//  *    this list of conditions and the following disclaimer in the documentation
//  *    and/or other materials provided with the distribution.
//  *
//  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//  */
const common = @import("common.zig");
pub const EDGE_I444_TOP_HAS_RIGHT = common.EDGE_I444_TOP_HAS_RIGHT;
pub const EDGE_I420_TOP_HAS_RIGHT = common.EDGE_I420_TOP_HAS_RIGHT;
pub const EDGE_I422_LEFT_HAS_BOTTOM = common.EDGE_I422_LEFT_HAS_BOTTOM;
pub const EDGE_I420_LEFT_HAS_BOTTOM = common.EDGE_I420_LEFT_HAS_BOTTOM;
pub const EDGE_ALL_TOP_HAS_RIGHT = common.EDGE_ALL_TOP_HAS_RIGHT;
pub const EDGE_ALL_LEFT_HAS_BOTTOM = common.EDGE_ALL_LEFT_HAS_BOTTOM;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
pub const struct_EdgeNode = extern struct {
    o: u8 = 0,
    h: [2]u8 = @import("std").mem.zeroes([2]u8),
    v: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const EdgeNode = struct_EdgeNode;
pub const struct_EdgeTip = extern struct {
    node: EdgeNode = @import("std").mem.zeroes(EdgeNode),
    split: [3]u8 = @import("std").mem.zeroes([3]u8),
};
pub const EdgeTip = struct_EdgeTip;
pub const struct_EdgeBranch = extern struct {
    node: EdgeNode = @import("std").mem.zeroes(EdgeNode),
    h4: u8 = 0,
    v4: u8 = 0,
    split_offset: [4]u16 = @import("std").mem.zeroes([4]u16),
};
pub const EdgeBranch = struct_EdgeBranch;
pub const BL_128X128 = common.BL_128X128;
pub const BL_64X64 = common.BL_64X64;
pub const BL_32X32 = common.BL_32X32;
pub const BL_16X16 = common.BL_16X16;
pub const BL_8X8 = common.BL_8X8;
pub const enum_BlockLevel = common.enum_BlockLevel;
pub const struct_ModeSelMem = extern struct {
    nwc: [3][*c]EdgeBranch = @import("std").mem.zeroes([3][*c]EdgeBranch),
    nt: [*c]EdgeTip = null,
};
const struct_unnamed_25 = extern struct {
    branch_sb128: [85]EdgeBranch = @import("std").mem.zeroes([85]EdgeBranch),
    tip_sb128: [256]EdgeTip = @import("std").mem.zeroes([256]EdgeTip),
    branch_sb64: [21]EdgeBranch = @import("std").mem.zeroes([21]EdgeBranch),
    tip_sb64: [64]EdgeTip = @import("std").mem.zeroes([64]EdgeTip),
};
pub var nodes: struct_unnamed_25 align(16) = @import("std").mem.zeroes(struct_unnamed_25);
pub export var antfly_av1_dav1d_intra_edge_tree: [2][*c]const EdgeNode = [2][*c]const EdgeNode{
    @ptrCast(@alignCast(@as([*c]EdgeBranch, @ptrCast(@alignCast(&nodes.branch_sb128))))),
    @ptrCast(@alignCast(@as([*c]EdgeBranch, @ptrCast(@alignCast(&nodes.branch_sb64))))),
};
pub fn init_edges(node: [*c]EdgeNode, bl: enum_BlockLevel, edge_flags: enum_EdgeFlags) callconv(.c) void {
    _ = &node;
    _ = &bl;
    _ = &edge_flags;
    node.*.o = @truncate(edge_flags);
    node.*.h[@as(c_int, 0)] = @truncate(edge_flags | @as(enum_EdgeFlags, EDGE_ALL_LEFT_HAS_BOTTOM));
    node.*.v[@as(c_int, 0)] = @truncate(edge_flags | @as(enum_EdgeFlags, EDGE_ALL_TOP_HAS_RIGHT));
    if (bl == @as(enum_BlockLevel, BL_8X8)) {
        const nt: [*c]EdgeTip = @ptrCast(@alignCast(node));
        _ = &nt;
        node.*.h[@as(c_int, 1)] = @truncate(edge_flags & @as(enum_EdgeFlags, @bitCast(EDGE_ALL_LEFT_HAS_BOTTOM | EDGE_I420_TOP_HAS_RIGHT)));
        node.*.v[@as(c_int, 1)] = @truncate(edge_flags & @as(enum_EdgeFlags, @bitCast((EDGE_ALL_TOP_HAS_RIGHT | EDGE_I420_LEFT_HAS_BOTTOM) | EDGE_I422_LEFT_HAS_BOTTOM)));
        nt.*.split[@as(c_int, 0)] = @truncate((edge_flags & @as(enum_EdgeFlags, EDGE_ALL_TOP_HAS_RIGHT)) | @as(enum_EdgeFlags, EDGE_I422_LEFT_HAS_BOTTOM));
        nt.*.split[@as(c_int, 1)] = @truncate(edge_flags | @as(enum_EdgeFlags, EDGE_I444_TOP_HAS_RIGHT));
        nt.*.split[@as(c_int, 2)] = @truncate(edge_flags & @as(enum_EdgeFlags, @bitCast((EDGE_I420_TOP_HAS_RIGHT | EDGE_I420_LEFT_HAS_BOTTOM) | EDGE_I422_LEFT_HAS_BOTTOM)));
    } else {
        const nwc: [*c]EdgeBranch = @ptrCast(@alignCast(node));
        _ = &nwc;
        node.*.h[@as(c_int, 1)] = @truncate(edge_flags & @as(enum_EdgeFlags, EDGE_ALL_LEFT_HAS_BOTTOM));
        node.*.v[@as(c_int, 1)] = @truncate(edge_flags & @as(enum_EdgeFlags, EDGE_ALL_TOP_HAS_RIGHT));
        nwc.*.h4 = EDGE_ALL_LEFT_HAS_BOTTOM;
        nwc.*.v4 = EDGE_ALL_TOP_HAS_RIGHT;
        if (bl == @as(enum_BlockLevel, BL_16X16)) {
            {
                const ref = &nwc.*.h4;
                ref.* = @bitCast(@as(i8, @truncate(@as(c_int, ref.*) | @as(c_int, @bitCast(edge_flags & @as(enum_EdgeFlags, EDGE_I420_TOP_HAS_RIGHT))))));
            }
            {
                const ref = &nwc.*.v4;
                ref.* = @bitCast(@as(i8, @truncate(@as(c_int, ref.*) | @as(c_int, @bitCast(edge_flags & @as(enum_EdgeFlags, @bitCast(EDGE_I420_LEFT_HAS_BOTTOM | EDGE_I422_LEFT_HAS_BOTTOM)))))));
            }
        }
    }
}
pub fn init_mode_node(nwc: [*c]EdgeBranch, bl: enum_BlockLevel, mem: [*c]struct_ModeSelMem, top_has_right: c_int, left_has_bottom: c_int) callconv(.c) void {
    _ = &nwc;
    _ = &bl;
    _ = &mem;
    _ = &top_has_right;
    _ = &left_has_bottom;
    init_edges(&nwc.*.node, bl, @bitCast((if (top_has_right != 0) EDGE_ALL_TOP_HAS_RIGHT else @as(c_int, 0)) | (if (left_has_bottom != 0) EDGE_ALL_LEFT_HAS_BOTTOM else @as(c_int, 0))));
    if (bl == @as(enum_BlockLevel, BL_16X16)) {
        {
            var n: c_int = 0;
            _ = &n;
            while (n < @as(c_int, 4)) : (n += 1) {
                const nt: [*c]EdgeTip = blk: {
                    const ref = &mem.*.nt;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                };
                _ = &nt;
                nwc.*.split_offset[@bitCast(@as(isize, @intCast(n)))] = @as(u16, @truncate(@as(usize, @intCast(@intFromPtr(nt))) -% @as(usize, @intCast(@intFromPtr(nwc)))));
                init_edges(&nt.*.node, bl +% @as(enum_BlockLevel, 1), @bitCast((if ((n == @as(c_int, 3)) or ((n == @as(c_int, 1)) and !(top_has_right != 0))) @as(c_int, 0) else EDGE_ALL_TOP_HAS_RIGHT) | (if (!((n == @as(c_int, 0)) or ((n == @as(c_int, 2)) and (left_has_bottom != 0)))) @as(c_int, 0) else EDGE_ALL_LEFT_HAS_BOTTOM)));
            }
        }
    } else {
        {
            var n: c_int = 0;
            _ = &n;
            while (n < @as(c_int, 4)) : (n += 1) {
                const nwc_child: [*c]EdgeBranch = blk: {
                    const ref = &mem.*.nwc[bl];
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                };
                _ = &nwc_child;
                nwc.*.split_offset[@bitCast(@as(isize, @intCast(n)))] = @as(u16, @truncate(@as(usize, @intCast(@intFromPtr(nwc_child))) -% @as(usize, @intCast(@intFromPtr(nwc)))));
                init_mode_node(nwc_child, bl +% @as(enum_BlockLevel, 1), mem, @as(c_int, @intFromBool(!((n == @as(c_int, 3)) or ((n == @as(c_int, 1)) and !(top_has_right != 0))))), @as(c_int, @intFromBool((n == @as(c_int, 0)) or ((n == @as(c_int, 2)) and (left_has_bottom != 0)))));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_init_intra_edge_tree() void {
    var mem: struct_ModeSelMem = undefined;
    _ = &mem;
    mem.nwc[BL_128X128] = &@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[@as(c_int, 1)];
    mem.nwc[BL_64X64] = &@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[@as(c_int, 1) + @as(c_int, 4)];
    mem.nwc[BL_32X32] = &@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[(@as(c_int, 1) + @as(c_int, 4)) + @as(c_int, 16)];
    mem.nt = @ptrCast(@alignCast(&nodes.tip_sb128));
    init_mode_node(@ptrCast(@alignCast(&nodes.branch_sb128)), BL_128X128, &mem, 1, 0);
    while (true) {
        if (!(mem.nwc[BL_128X128] == (&@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[@as(c_int, 1) + @as(c_int, 4)]))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(mem.nwc[BL_64X64] == (&@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[(@as(c_int, 1) + @as(c_int, 4)) + @as(c_int, 16)]))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(mem.nwc[BL_32X32] == (&@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb128))[((@as(c_int, 1) + @as(c_int, 4)) + @as(c_int, 16)) + @as(c_int, 64)]))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(mem.nt == (&@as([*]EdgeTip, @ptrCast(&nodes.tip_sb128))[@as(c_int, 256)]))) {
            unreachable;
        }
        if (!false) break;
    }
    mem.nwc[BL_128X128] = null;
    mem.nwc[BL_64X64] = &@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb64))[@as(c_int, 1)];
    mem.nwc[BL_32X32] = &@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb64))[@as(c_int, 1) + @as(c_int, 4)];
    mem.nt = @ptrCast(@alignCast(&nodes.tip_sb64));
    init_mode_node(@ptrCast(@alignCast(&nodes.branch_sb64)), BL_64X64, &mem, 1, 0);
    while (true) {
        if (!(mem.nwc[BL_64X64] == (&@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb64))[@as(c_int, 1) + @as(c_int, 4)]))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(mem.nwc[BL_32X32] == (&@as([*]EdgeBranch, @ptrCast(&nodes.branch_sb64))[(@as(c_int, 1) + @as(c_int, 4)) + @as(c_int, 16)]))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(mem.nt == (&@as([*]EdgeTip, @ptrCast(&nodes.tip_sb64))[@as(c_int, 64)]))) {
            unreachable;
        }
        if (!false) break;
    }
}
