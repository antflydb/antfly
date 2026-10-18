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
pub const struct___va_list_1 = common.struct___va_list_1;
pub const __builtin_va_list = struct___va_list_1;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const vpx_img_fmt_t = common.vpx_img_fmt_t;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const struct_vpx_image = extern struct {
    fmt: vpx_img_fmt_t = @import("std").mem.zeroes(vpx_img_fmt_t),
    cs: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    w: c_uint = 0,
    h: c_uint = 0,
    bit_depth: c_uint = 0,
    d_w: c_uint = 0,
    d_h: c_uint = 0,
    r_w: c_uint = 0,
    r_h: c_uint = 0,
    x_chroma_shift: c_uint = 0,
    y_chroma_shift: c_uint = 0,
    planes: [4][*c]u8 = @import("std").mem.zeroes([4][*c]u8),
    stride: [4]c_int = @import("std").mem.zeroes([4]c_int),
    bps: c_int = 0,
    user_priv: ?*anyopaque = null,
    img_data: [*c]u8 = null,
    img_data_owner: c_int = 0,
    self_allocd: c_int = 0,
    fb_priv: ?*anyopaque = null,
};
pub const vpx_image_t = struct_vpx_image;
pub const vpx_image_rect_t = common.vpx_image_rect_t;
pub const VPX_CODEC_ERROR = common.VPX_CODEC_ERROR;
pub const VPX_CODEC_ABI_MISMATCH = common.VPX_CODEC_ABI_MISMATCH;
pub const VPX_CODEC_INCAPABLE = common.VPX_CODEC_INCAPABLE;
pub const VPX_CODEC_INVALID_PARAM = common.VPX_CODEC_INVALID_PARAM;
pub const vpx_codec_err_t = common.vpx_codec_err_t;
pub const vpx_codec_caps_t = common.vpx_codec_caps_t;
pub const vpx_codec_flags_t = common.vpx_codec_flags_t;
pub const vpx_codec_iface_t = struct_vpx_codec_iface;
pub const struct_vpx_codec_dec_cfg = common.struct_vpx_codec_dec_cfg;
pub const vpx_fixed_buf_t = common.vpx_fixed_buf_t;
pub const struct_vpx_codec_enc_cfg = common.struct_vpx_codec_enc_cfg;
const union_unnamed_2 = extern union {
    dec: [*c]const struct_vpx_codec_dec_cfg,
    enc: [*c]const struct_vpx_codec_enc_cfg,
    raw: ?*const anyopaque,
};
pub const vpx_codec_put_frame_cb_fn_t = ?*const fn (user_priv: ?*anyopaque, img: [*c]const vpx_image_t) callconv(.c) void;
pub const vpx_codec_put_slice_cb_fn_t = ?*const fn (user_priv: ?*anyopaque, img: [*c]const vpx_image_t, valid: [*c]const vpx_image_rect_t, update: [*c]const vpx_image_rect_t) callconv(.c) void;
const union_unnamed_4 = extern union {
    put_frame: vpx_codec_put_frame_cb_fn_t,
    put_slice: vpx_codec_put_slice_cb_fn_t,
};
pub const struct_vpx_codec_priv_cb_pair = extern struct {
    u: union_unnamed_4 = @import("std").mem.zeroes(union_unnamed_4),
    user_priv: ?*anyopaque = null,
};
pub const vpx_codec_priv_cb_pair_t = struct_vpx_codec_priv_cb_pair;
const struct_unnamed_3 = extern struct {
    put_frame_cb: vpx_codec_priv_cb_pair_t = @import("std").mem.zeroes(vpx_codec_priv_cb_pair_t),
    put_slice_cb: vpx_codec_priv_cb_pair_t = @import("std").mem.zeroes(vpx_codec_priv_cb_pair_t),
};
pub const enum_vpx_codec_cx_pkt_kind = common.enum_vpx_codec_cx_pkt_kind;
pub const vpx_codec_pts_t = common.vpx_codec_pts_t;
pub const vpx_codec_frame_flags_t = common.vpx_codec_frame_flags_t;
const struct_unnamed_7 = extern struct {
    buf: ?*anyopaque = null,
    sz: usize = 0,
    pts: vpx_codec_pts_t = 0,
    duration: c_ulong = 0,
    flags: vpx_codec_frame_flags_t = 0,
    partition_id: c_int = 0,
    width: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    height: [5]c_uint = @import("std").mem.zeroes([5]c_uint),
    spatial_layer_encoded: [5]u8 = @import("std").mem.zeroes([5]u8),
};
pub const struct_vpx_psnr_pkt_8 = common.struct_vpx_psnr_pkt_8;
const union_unnamed_6 = extern union {
    frame: struct_unnamed_7,
    twopass_stats: vpx_fixed_buf_t,
    firstpass_mb_stats: vpx_fixed_buf_t,
    psnr: struct_vpx_psnr_pkt_8,
    raw: vpx_fixed_buf_t,
    pad: [124]u8,
};
pub const struct_vpx_codec_cx_pkt = extern struct {
    kind: enum_vpx_codec_cx_pkt_kind = @import("std").mem.zeroes(enum_vpx_codec_cx_pkt_kind),
    data: union_unnamed_6 = @import("std").mem.zeroes(union_unnamed_6),
};
pub const vpx_codec_cx_pkt_t = struct_vpx_codec_cx_pkt;
const struct_unnamed_5 = extern struct {
    cx_data_dst_buf: vpx_fixed_buf_t = @import("std").mem.zeroes(vpx_fixed_buf_t),
    cx_data_pad_before: c_uint = 0,
    cx_data_pad_after: c_uint = 0,
    cx_data_pkt: vpx_codec_cx_pkt_t = @import("std").mem.zeroes(vpx_codec_cx_pkt_t),
    total_encoders: c_uint = 0,
};
pub const struct_vpx_codec_priv = extern struct {
    err_detail: [*c]const u8 = null,
    init_flags: vpx_codec_flags_t = 0,
    dec: struct_unnamed_3 = @import("std").mem.zeroes(struct_unnamed_3),
    enc: struct_unnamed_5 = @import("std").mem.zeroes(struct_unnamed_5),
};
pub const vpx_codec_priv_t = struct_vpx_codec_priv;
pub const struct_vpx_codec_ctx = extern struct {
    name: [*c]const u8 = null,
    iface: [*c]const vpx_codec_iface_t = null,
    err: vpx_codec_err_t = @import("std").mem.zeroes(vpx_codec_err_t),
    err_detail: [*c]const u8 = null,
    init_flags: vpx_codec_flags_t = 0,
    config: union_unnamed_2 = @import("std").mem.zeroes(union_unnamed_2),
    priv: [*c]vpx_codec_priv_t = null,
};
pub const vpx_codec_ctx_t = struct_vpx_codec_ctx;
pub const vpx_codec_priv_enc_mr_cfg_t = common.vpx_codec_priv_enc_mr_cfg_t;
pub const vpx_codec_init_fn_t = ?*const fn (ctx: [*c]vpx_codec_ctx_t, data: [*c]vpx_codec_priv_enc_mr_cfg_t) callconv(.c) vpx_codec_err_t;
pub const struct_vpx_codec_alg_priv = opaque {};
pub const vpx_codec_alg_priv_t = struct_vpx_codec_alg_priv;
pub const vpx_codec_destroy_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t) callconv(.c) vpx_codec_err_t;
pub const va_list = __builtin_va_list;
pub const vpx_codec_control_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, ap: va_list) callconv(.c) vpx_codec_err_t;
pub const struct_vpx_codec_ctrl_fn_map = extern struct {
    ctrl_id: c_int = 0,
    @"fn": vpx_codec_control_fn_t = null,
};
pub const vpx_codec_ctrl_fn_map_t = struct_vpx_codec_ctrl_fn_map;
pub const vpx_codec_stream_info_t = common.vpx_codec_stream_info_t;
pub const vpx_codec_peek_si_fn_t = common.vpx_codec_peek_si_fn_t;
pub const vpx_codec_get_si_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, si: [*c]vpx_codec_stream_info_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_decode_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, data: [*c]const u8, data_sz: c_uint, user_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_iter_t = common.vpx_codec_iter_t;
pub const vpx_codec_get_frame_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, iter: [*c]vpx_codec_iter_t) callconv(.c) [*c]vpx_image_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
pub const vpx_codec_set_fb_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, cb_get: vpx_get_frame_buffer_cb_fn_t, cb_release: vpx_release_frame_buffer_cb_fn_t, cb_priv: ?*anyopaque) callconv(.c) vpx_codec_err_t;
pub const struct_vpx_codec_dec_iface_9 = extern struct {
    peek_si: vpx_codec_peek_si_fn_t = null,
    get_si: vpx_codec_get_si_fn_t = null,
    decode: vpx_codec_decode_fn_t = null,
    get_frame: vpx_codec_get_frame_fn_t = null,
    set_fb_fn: vpx_codec_set_fb_fn_t = null,
};
pub const vpx_codec_enc_cfg_t = common.vpx_codec_enc_cfg_t;
pub const vpx_codec_enc_cfg_map_t = common.vpx_codec_enc_cfg_map_t;
pub const vpx_enc_frame_flags_t = common.vpx_enc_frame_flags_t;
pub const vpx_enc_deadline_t = common.vpx_enc_deadline_t;
pub const vpx_codec_encode_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, img: [*c]const vpx_image_t, pts: vpx_codec_pts_t, duration: c_ulong, flags: vpx_enc_frame_flags_t, deadline: vpx_enc_deadline_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_get_cx_data_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, iter: [*c]vpx_codec_iter_t) callconv(.c) [*c]const vpx_codec_cx_pkt_t;
pub const vpx_codec_enc_config_set_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t, cfg: [*c]const vpx_codec_enc_cfg_t) callconv(.c) vpx_codec_err_t;
pub const vpx_codec_get_global_headers_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t) callconv(.c) [*c]vpx_fixed_buf_t;
pub const vpx_codec_get_preview_frame_fn_t = ?*const fn (ctx: ?*vpx_codec_alg_priv_t) callconv(.c) [*c]vpx_image_t;
pub const vpx_codec_enc_mr_get_mem_loc_fn_t = common.vpx_codec_enc_mr_get_mem_loc_fn_t;
pub const struct_vpx_codec_enc_iface_10 = extern struct {
    cfg_map_count: c_int = 0,
    cfg_maps: [*c]const vpx_codec_enc_cfg_map_t = null,
    encode: vpx_codec_encode_fn_t = null,
    get_cx_data: vpx_codec_get_cx_data_fn_t = null,
    cfg_set: vpx_codec_enc_config_set_fn_t = null,
    get_glob_hdrs: vpx_codec_get_global_headers_fn_t = null,
    get_preview: vpx_codec_get_preview_frame_fn_t = null,
    mr_get_mem_loc: vpx_codec_enc_mr_get_mem_loc_fn_t = null,
};
pub const struct_vpx_codec_iface = extern struct {
    name: [*c]const u8 = null,
    abi_version: c_int = 0,
    caps: vpx_codec_caps_t = 0,
    init: vpx_codec_init_fn_t = null,
    destroy: vpx_codec_destroy_fn_t = null,
    ctrl_maps: [*c]const vpx_codec_ctrl_fn_map_t = null,
    dec: struct_vpx_codec_dec_iface_9 = @import("std").mem.zeroes(struct_vpx_codec_dec_iface_9),
    enc: struct_vpx_codec_enc_iface_10 = @import("std").mem.zeroes(struct_vpx_codec_enc_iface_10),
};
pub extern fn antfly_vp9_vpx_codec_destroy(ctx: [*c]vpx_codec_ctx_t) vpx_codec_err_t;
pub const vpx_codec_dec_cfg_t = common.vpx_codec_dec_cfg_t;
pub fn get_alg_priv(arg_ctx: [*c]vpx_codec_ctx_t) callconv(.c) ?*vpx_codec_alg_priv_t {
    var ctx = arg_ctx;
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return @ptrCast(@alignCast(ctx.*.priv));
}
pub export fn antfly_vp9_vpx_codec_dec_init_ver(arg_ctx: [*c]vpx_codec_ctx_t, arg_iface: [*c]const vpx_codec_iface_t, arg_cfg: [*c]const vpx_codec_dec_cfg_t, arg_flags: vpx_codec_flags_t, arg_ver: c_int) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var iface = arg_iface;
    _ = &iface;
    var cfg = arg_cfg;
    _ = &cfg;
    var flags = arg_flags;
    _ = &flags;
    var ver = arg_ver;
    _ = &ver;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var res: vpx_codec_err_t = undefined;
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (ver != (@as(c_int, 3) + (@as(c_int, 4) + @as(c_int, 5)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        res = VPX_CODEC_ABI_MISMATCH;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(ctx != null) or !(iface != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            res = VPX_CODEC_INVALID_PARAM;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (iface.*.abi_version != @as(c_int, 5)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                res = VPX_CODEC_ABI_MISMATCH;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (((flags & @as(vpx_codec_flags_t, 65536)) != 0) and (@as(vpx_codec_flags_t, @as(c_int, @intFromBool(!((iface.*.caps & @as(vpx_codec_caps_t, 262144)) != 0)))) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    res = VPX_CODEC_INCAPABLE;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (((flags & @as(vpx_codec_flags_t, 131072)) != 0) and (@as(vpx_codec_flags_t, @as(c_int, @intFromBool(!((iface.*.caps & @as(vpx_codec_caps_t, 524288)) != 0)))) != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        res = VPX_CODEC_INCAPABLE;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (((flags & @as(vpx_codec_flags_t, 262144)) != 0) and (@as(vpx_codec_flags_t, @as(c_int, @intFromBool(!((iface.*.caps & @as(vpx_codec_caps_t, 1048576)) != 0)))) != 0)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            res = VPX_CODEC_INCAPABLE;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (!((iface.*.caps & @as(vpx_codec_caps_t, 1)) != 0)) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                res = VPX_CODEC_INCAPABLE;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            } else {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(ctx)), 0, @sizeOf(@TypeOf(ctx.*)));
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                ctx.*.iface = iface;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                ctx.*.name = iface.*.name;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                ctx.*.priv = null;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                ctx.*.init_flags = flags;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                ctx.*.config.dec = cfg;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                res = ctx.*.iface.*.init.?(ctx, null);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                                if (res != 0) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                    ctx.*.err_detail = @ptrCast(@alignCast(if (ctx.*.priv != null) @as(?*const anyopaque, @ptrCast(@alignCast(ctx.*.priv.*.err_detail))) else @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null))))));
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                    _ = antfly_vp9_vpx_codec_destroy(ctx);
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return 0;
                                    }
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return 0;
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return if (ctx != null) blk: {
        const tmp = res;
        ctx.*.err = tmp;
        break :blk tmp;
    } else res;
}
pub export fn antfly_vp9_vpx_codec_decode(arg_ctx: [*c]vpx_codec_ctx_t, arg_data: [*c]const u8, arg_data_sz: c_uint, arg_user_priv: ?*anyopaque, arg_deadline: c_long) vpx_codec_err_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var data = arg_data;
    _ = &data;
    var data_sz = arg_data_sz;
    _ = &data_sz;
    var user_priv = arg_user_priv;
    _ = &user_priv;
    var deadline = arg_deadline;
    _ = &deadline;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var res: vpx_codec_err_t = undefined;
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &deadline;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((!(ctx != null) or ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(!(data != null)))))) != 0) and (data_sz != 0))) or ((data != null) and !(data_sz != 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        res = VPX_CODEC_INVALID_PARAM;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(ctx.*.iface != null) or !(ctx.*.priv != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            res = VPX_CODEC_ERROR;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            res = ctx.*.iface.*.dec.decode.?(get_alg_priv(ctx), data, data_sz, user_priv);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return if (ctx != null) blk: {
        const tmp = res;
        ctx.*.err = tmp;
        break :blk tmp;
    } else res;
}
pub export fn antfly_vp9_vpx_codec_get_frame(arg_ctx: [*c]vpx_codec_ctx_t, arg_iter: [*c]vpx_codec_iter_t) [*c]vpx_image_t {
    var ctx = arg_ctx;
    _ = &ctx;
    var iter = arg_iter;
    _ = &iter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var img: [*c]vpx_image_t = undefined;
    _ = &img;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (((!(ctx != null) or !(iter != null)) or !(ctx.*.iface != null)) or !(ctx.*.priv != null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        img = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        img = ctx.*.iface.*.dec.get_frame.?(get_alg_priv(ctx), iter);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    return img;
}
