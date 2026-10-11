// Copyright (c) 2010, The WebM Project authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in
//     the documentation and/or other materials provided with the
//     distribution.
//
// Neither the name of Google, nor the WebM Project, nor the names
//     of its contributors may be used to endorse or promote products
//     derived from this software without specific prior written
//     permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-3-Clause
// /*
//  *  Copyright (c) 2013 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const vpx_prob = common.vpx_prob;
pub const vpx_tree_index = common.vpx_tree_index;
pub fn get_prob(arg_num: c_uint, arg_den: c_uint) callconv(.c) vpx_prob {
    var num = arg_num;
    _ = &num;
    var den = arg_den;
    _ = &den;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const p: c_int = @bitCast(@as(c_uint, @truncate(((@as(u64, num) *% @as(u64, 256)) +% @as(u64, den >> @intCast(@as(c_uint, 1)))) / @as(u64, den))));
        _ = &p;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const clipped_prob: c_int = (p | ((@as(c_int, 255) - p) >> @intCast(@as(c_int, 23)))) | @as(c_int, @intFromBool(p == @as(c_int, 0)));
        _ = &clipped_prob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(@as(i8, @truncate(clipped_prob)));
    }
}
pub fn weighted_prob(arg_prob1: c_int, arg_prob2: c_int, arg_factor: c_int) callconv(.c) vpx_prob {
    var prob1 = arg_prob1;
    _ = &prob1;
    var prob2 = arg_prob2;
    _ = &prob2;
    var factor = arg_factor;
    _ = &factor;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(i8, @truncate((((prob1 * (@as(c_int, 256) - factor)) + (prob2 * factor)) + (@as(c_int, 1) << @intCast(@as(c_int, 8) - @as(c_int, 1)))) >> @intCast(@as(c_int, 8)))));
}
pub const count_to_update_factor = common.count_to_update_factor;
pub fn mode_mv_merge_probs(arg_pre_prob: vpx_prob, arg_ct: [*c]const c_uint) callconv(.c) vpx_prob {
    var pre_prob = arg_pre_prob;
    _ = &pre_prob;
    var ct = arg_ct;
    _ = &ct;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const den: c_uint = ct[@as(c_int, 0)] +% ct[@as(c_int, 1)];
    _ = &den;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (den == @as(c_uint, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return pre_prob;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const count: c_uint = if (den < @as(c_uint, @bitCast(@as(c_int, @as(c_int, 20))))) den else @as(c_uint, @bitCast(@as(c_int, @as(c_int, 20))));
        _ = &count;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const factor: c_uint = @bitCast(@as(c_int, count_to_update_factor[count]));
        _ = &factor;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const prob: vpx_prob = get_prob(ct[@as(c_int, 0)], den);
        _ = &prob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return weighted_prob(pre_prob, prob, @bitCast(@as(c_uint, @truncate(factor))));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub export const antfly_vp9_vpx_norm: [256]u8 align(16) = [256]u8{
    0,
    7,
    6,
    6,
    5,
    5,
    5,
    5,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    3,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    2,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
};
pub fn tree_merge_probs_impl(arg_i: c_uint, arg_tree: [*c]const vpx_tree_index, arg_pre_probs: [*c]const vpx_prob, arg_counts: [*c]const c_uint, arg_probs: [*c]vpx_prob) callconv(.c) c_uint {
    var i = arg_i;
    _ = &i;
    var tree = arg_tree;
    _ = &tree;
    var pre_probs = arg_pre_probs;
    _ = &pre_probs;
    var counts = arg_counts;
    _ = &counts;
    var probs = arg_probs;
    _ = &probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const l: c_int = tree[i];
    _ = &l;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const left_count: c_uint = if (l <= @as(c_int, 0)) counts[@bitCast(@as(isize, @intCast(-l)))] else tree_merge_probs_impl(@bitCast(@as(c_int, l)), tree, pre_probs, counts, probs);
    _ = &left_count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const r: c_int = tree[i +% @as(c_uint, 1)];
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const right_count: c_uint = if (r <= @as(c_int, 0)) counts[@bitCast(@as(isize, @intCast(-r)))] else tree_merge_probs_impl(@bitCast(@as(c_int, r)), tree, pre_probs, counts, probs);
    _ = &right_count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const ct: [2]c_uint = [2]c_uint{
        left_count,
        right_count,
    };
    _ = &ct;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    probs[i >> @intCast(@as(c_uint, 1))] = mode_mv_merge_probs(pre_probs[i >> @intCast(@as(c_uint, 1))], @ptrCast(@alignCast(&ct)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return left_count +% right_count;
}
pub export fn antfly_vp9_vpx_tree_merge_probs(arg_tree: [*c]const vpx_tree_index, arg_pre_probs: [*c]const vpx_prob, arg_counts: [*c]const c_uint, arg_probs: [*c]vpx_prob) void {
    var tree = arg_tree;
    _ = &tree;
    var pre_probs = arg_pre_probs;
    _ = &pre_probs;
    var counts = arg_counts;
    _ = &counts;
    var probs = arg_probs;
    _ = &probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = tree_merge_probs_impl(0, tree, pre_probs, counts, probs);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
