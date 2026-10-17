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
//  *  Copyright (c) 2010 The WebM project authors. All Rights Reserved.
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
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const SEG_LVL_FEATURES = common.SEG_LVL_FEATURES;
pub const struct_segmentation = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    update_data: u8 = 0,
    abs_delta: u8 = 0,
    temporal_update: u8 = 0,
    tree_probs: [7]vpx_prob = @import("std").mem.zeroes([7]vpx_prob),
    pred_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    feature_data: [8][4]i16 = @import("std").mem.zeroes([8][4]i16),
    feature_mask: [8]u32 = @import("std").mem.zeroes([8]u32),
    aq_av_offset: c_int = 0,
};
pub const seg_feature_data_signed: [4]c_int = [4]c_int{
    1,
    1,
    0,
    0,
};
pub const seg_feature_data_max: [4]c_int = [4]c_int{
    255,
    63,
    3,
    0,
};
pub export fn antfly_vp9_vp9_clearall_segfeatures(arg_seg: [*c]struct_segmentation) void {
    var seg = arg_seg;
    _ = &seg;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&seg.*.feature_data)), 0, @sizeOf(@TypeOf(seg.*.feature_data)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&seg.*.feature_mask)), 0, @sizeOf(@TypeOf(seg.*.feature_mask)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.aq_av_offset = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_enable_segfeature(arg_seg: [*c]struct_segmentation, arg_segment_id: c_int, arg_feature_id: SEG_LVL_FEATURES) void {
    var seg = arg_seg;
    _ = &seg;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var feature_id = arg_feature_id;
    _ = &feature_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.feature_mask[@bitCast(@as(isize, @intCast(segment_id)))] |= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, @bitCast(feature_id)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_seg_feature_data_max(arg_feature_id: SEG_LVL_FEATURES) c_int {
    var feature_id = arg_feature_id;
    _ = &feature_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return seg_feature_data_max[feature_id];
}
pub export fn antfly_vp9_vp9_is_segfeature_signed(arg_feature_id: SEG_LVL_FEATURES) c_int {
    var feature_id = arg_feature_id;
    _ = &feature_id;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return seg_feature_data_signed[feature_id];
}
pub export fn antfly_vp9_vp9_set_segdata(arg_seg: [*c]struct_segmentation, arg_segment_id: c_int, arg_feature_id: SEG_LVL_FEATURES, arg_seg_data: c_int) void {
    var seg = arg_seg;
    _ = &seg;
    var segment_id = arg_segment_id;
    _ = &segment_id;
    var feature_id = arg_feature_id;
    _ = &feature_id;
    var seg_data = arg_seg_data;
    _ = &seg_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (seg_data < @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.feature_data[@bitCast(@as(isize, @intCast(segment_id)))][feature_id] = @truncate(seg_data);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export const antfly_vp9_vp9_segment_tree: [14]vpx_tree_index = [14]vpx_tree_index{
    2,
    4,
    6,
    8,
    10,
    12,
    0,
    @truncate(-@as(c_int, 1)),
    @truncate(-@as(c_int, 2)),
    @truncate(-@as(c_int, 3)),
    @truncate(-@as(c_int, 4)),
    @truncate(-@as(c_int, 5)),
    @truncate(-@as(c_int, 6)),
    @truncate(-@as(c_int, 7)),
};
