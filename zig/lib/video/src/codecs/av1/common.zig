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
pub const ADST: c_int = 1;
pub const ADST_ADST: c_int = 3;
pub const ADST_DCT: c_int = 1;
pub const ADST_FLIPADST: c_int = 7;
pub const Av1Filter = struct_Av1Filter;
pub const Av1FilterLUT = struct_Av1FilterLUT;
pub const Av1Restoration = struct_Av1Restoration;
pub const Av1RestorationUnit = struct_Av1RestorationUnit;
pub const BL_128X128: c_int = 0;
pub const BL_16X16: c_int = 3;
pub const BL_32X32: c_int = 2;
pub const BL_64X64: c_int = 1;
pub const BL_8X8: c_int = 4;
pub const BS_128x128: c_int = 0;
pub const BS_128x64: c_int = 1;
pub const BS_16x16: c_int = 12;
pub const BS_16x32: c_int = 11;
pub const BS_16x4: c_int = 14;
pub const BS_16x64: c_int = 10;
pub const BS_16x8: c_int = 13;
pub const BS_32x16: c_int = 8;
pub const BS_32x32: c_int = 7;
pub const BS_32x64: c_int = 6;
pub const BS_32x8: c_int = 9;
pub const BS_4x16: c_int = 19;
pub const BS_4x4: c_int = 21;
pub const BS_4x8: c_int = 20;
pub const BS_64x128: c_int = 2;
pub const BS_64x16: c_int = 5;
pub const BS_64x32: c_int = 4;
pub const BS_64x64: c_int = 3;
pub const BS_8x16: c_int = 16;
pub const BS_8x32: c_int = 15;
pub const BS_8x4: c_int = 18;
pub const BS_8x8: c_int = 17;
pub const CDEF_HAVE_BOTTOM: c_int = 8;
pub const CDEF_HAVE_LEFT: c_int = 1;
pub const CDEF_HAVE_RIGHT: c_int = 2;
pub const CDEF_HAVE_TOP: c_int = 4;
pub const CFL_PRED: c_int = 13;
pub const COMP_INTER_AVG: c_int = 2;
pub const COMP_INTER_NONE: c_int = 0;
pub const COMP_INTER_SEG: c_int = 3;
pub const COMP_INTER_WEDGE: c_int = 4;
pub const COMP_INTER_WEIGHTED_AVG: c_int = 1;
pub const CdfCoefContext = struct_CdfCoefContext;
pub const CdfMvComponent = struct_CdfMvComponent;
pub const CdfMvContext = struct_CdfMvContext;
pub const DAV1D_ADAPTIVE: c_int = 2;
pub const DAV1D_CHR_UNKNOWN: c_int = 0;
pub const DAV1D_COLOR_PRI_BT709: c_int = 1;
pub const DAV1D_COLOR_PRI_UNKNOWN: c_int = 2;
pub const DAV1D_DECODEFRAMETYPE_ALL: c_int = 0;
pub const DAV1D_DECODEFRAMETYPE_INTRA: c_int = 2;
pub const DAV1D_DECODEFRAMETYPE_KEY: c_int = 3;
pub const DAV1D_DECODEFRAMETYPE_REFERENCE: c_int = 1;
pub const DAV1D_EVENT_FLAG_NEW_OP_PARAMS_INFO: c_int = 2;
pub const DAV1D_EVENT_FLAG_NEW_SEQUENCE: c_int = 1;
pub const DAV1D_FILTER_8TAP_REGULAR: c_int = 0;
pub const DAV1D_FILTER_8TAP_SHARP: c_int = 2;
pub const DAV1D_FILTER_8TAP_SMOOTH: c_int = 1;
pub const DAV1D_FILTER_BILINEAR: c_int = 3;
pub const DAV1D_FILTER_SWITCHABLE: c_int = 4;
pub const DAV1D_FRAME_TYPE_INTER: c_int = 1;
pub const DAV1D_FRAME_TYPE_INTRA: c_int = 2;
pub const DAV1D_FRAME_TYPE_KEY: c_int = 0;
pub const DAV1D_FRAME_TYPE_SWITCH: c_int = 3;
pub const DAV1D_INLOOPFILTER_ALL: c_int = 7;
pub const DAV1D_INLOOPFILTER_CDEF: c_int = 2;
pub const DAV1D_INLOOPFILTER_DEBLOCK: c_int = 1;
pub const DAV1D_INLOOPFILTER_RESTORATION: c_int = 4;
pub const DAV1D_MC_IDENTITY: c_int = 0;
pub const DAV1D_MC_UNKNOWN: c_int = 2;
pub const DAV1D_N_SWITCHABLE_FILTERS: c_int = 3;
pub const DAV1D_OBU_FRAME: c_int = 6;
pub const DAV1D_OBU_FRAME_HDR: c_int = 3;
pub const DAV1D_OBU_METADATA: c_int = 5;
pub const DAV1D_OBU_PADDING: c_int = 15;
pub const DAV1D_OBU_REDUNDANT_FRAME_HDR: c_int = 7;
pub const DAV1D_OBU_SEQ_HDR: c_int = 1;
pub const DAV1D_OBU_TD: c_int = 2;
pub const DAV1D_OBU_TILE_GRP: c_int = 4;
pub const DAV1D_PIXEL_LAYOUT_I400: c_int = 0;
pub const DAV1D_PIXEL_LAYOUT_I420: c_int = 1;
pub const DAV1D_PIXEL_LAYOUT_I422: c_int = 2;
pub const DAV1D_PIXEL_LAYOUT_I444: c_int = 3;
pub const DAV1D_RESTORATION_NONE: c_int = 0;
pub const DAV1D_RESTORATION_SGRPROJ: c_int = 3;
pub const DAV1D_RESTORATION_SWITCHABLE: c_int = 1;
pub const DAV1D_RESTORATION_WIENER: c_int = 2;
pub const DAV1D_TASK_TYPE_CDEF: c_int = 7;
pub const DAV1D_TASK_TYPE_DEBLOCK_COLS: c_int = 5;
pub const DAV1D_TASK_TYPE_DEBLOCK_ROWS: c_int = 6;
pub const DAV1D_TASK_TYPE_ENTROPY_PROGRESS: c_int = 3;
pub const DAV1D_TASK_TYPE_FG_APPLY: c_int = 12;
pub const DAV1D_TASK_TYPE_FG_PREP: c_int = 11;
pub const DAV1D_TASK_TYPE_INIT: c_int = 0;
pub const DAV1D_TASK_TYPE_INIT_CDF: c_int = 1;
pub const DAV1D_TASK_TYPE_LOOP_RESTORATION: c_int = 9;
pub const DAV1D_TASK_TYPE_RECONSTRUCTION_PROGRESS: c_int = 10;
pub const DAV1D_TASK_TYPE_SUPER_RESOLUTION: c_int = 8;
pub const DAV1D_TASK_TYPE_TILE_ENTROPY: c_int = 2;
pub const DAV1D_TASK_TYPE_TILE_RECONSTRUCTION: c_int = 4;
pub const DAV1D_TRC_SRGB: c_int = 13;
pub const DAV1D_TRC_UNKNOWN: c_int = 2;
pub const DAV1D_TX_4X4_ONLY: c_int = 0;
pub const DAV1D_TX_LARGEST: c_int = 1;
pub const DAV1D_TX_SWITCHABLE: c_int = 2;
pub const DAV1D_WM_TYPE_AFFINE: c_int = 3;
pub const DAV1D_WM_TYPE_IDENTITY: c_int = 0;
pub const DAV1D_WM_TYPE_ROT_ZOOM: c_int = 2;
pub const DAV1D_WM_TYPE_TRANSLATION: c_int = 1;
pub const DCT: c_int = 0;
pub const DCT_ADST: c_int = 2;
pub const DCT_DCT: c_int = 0;
pub const DCT_FLIPADST: c_int = 5;
pub const DC_128_PRED: c_int = 5;
pub const DC_PRED: c_int = 0;
pub const DIAG_DOWN_LEFT_PRED: c_int = 3;
pub const DIAG_DOWN_RIGHT_PRED: c_int = 4;
pub const Dav1dContentLightLevel = struct_Dav1dContentLightLevel;
pub const Dav1dFilmGrainData = struct_Dav1dFilmGrainData;
pub const Dav1dITUTT35 = struct_Dav1dITUTT35;
pub const Dav1dLoopfilterModeRefDeltas = struct_Dav1dLoopfilterModeRefDeltas;
pub const Dav1dMasteringDisplay = struct_Dav1dMasteringDisplay;
pub const Dav1dMemPoolBuffer = struct_Dav1dMemPoolBuffer;
pub const Dav1dPalDSPContext = struct_Dav1dPalDSPContext;
pub const Dav1dPictureParameters = struct_Dav1dPictureParameters;
pub const Dav1dSegmentationData = struct_Dav1dSegmentationData;
pub const Dav1dSegmentationDataSet = struct_Dav1dSegmentationDataSet;
pub const EDGE_ALL_LEFT_HAS_BOTTOM: c_int = 56;
pub const EDGE_ALL_TOP_HAS_RIGHT: c_int = 7;
pub const EDGE_ALL_TR_AND_BL: c_int = 63;
pub const EDGE_I420_LEFT_HAS_BOTTOM: c_int = 32;
pub const EDGE_I420_TOP_HAS_RIGHT: c_int = 4;
pub const EDGE_I422_LEFT_HAS_BOTTOM: c_int = 16;
pub const EDGE_I444_LEFT_HAS_BOTTOM: c_int = 8;
pub const EDGE_I444_TOP_HAS_RIGHT: c_int = 1;
pub const FILTER_2D_8TAP_REGULAR: c_int = 0;
pub const FILTER_2D_8TAP_REGULAR_SHARP: c_int = 2;
pub const FILTER_2D_8TAP_REGULAR_SMOOTH: c_int = 1;
pub const FILTER_2D_8TAP_SHARP: c_int = 5;
pub const FILTER_2D_8TAP_SHARP_REGULAR: c_int = 3;
pub const FILTER_2D_8TAP_SHARP_SMOOTH: c_int = 4;
pub const FILTER_2D_8TAP_SMOOTH: c_int = 7;
pub const FILTER_2D_8TAP_SMOOTH_REGULAR: c_int = 6;
pub const FILTER_2D_8TAP_SMOOTH_SHARP: c_int = 8;
pub const FILTER_2D_BILINEAR: c_int = 9;
pub const FILTER_PRED: c_int = 13;
pub const FLIPADST: c_int = 3;
pub const FLIPADST_ADST: c_int = 8;
pub const FLIPADST_DCT: c_int = 4;
pub const FLIPADST_FLIPADST: c_int = 6;
pub const GLOBALMV: c_int = 2;
pub const GLOBALMV_GLOBALMV: c_int = 6;
pub const HOR_DOWN_PRED: c_int = 6;
pub const HOR_PRED: c_int = 2;
pub const HOR_UP_PRED: c_int = 7;
pub const H_ADST: c_int = 13;
pub const H_DCT: c_int = 11;
pub const H_FLIPADST: c_int = 15;
pub const IDENTITY: c_int = 2;
pub const IDTX: c_int = 9;
pub const II_DC_PRED: c_int = 0;
pub const II_SMOOTH_PRED: c_int = 3;
pub const INTER_INTRA_BLEND: c_int = 1;
pub const INTER_INTRA_NONE: c_int = 0;
pub const INTER_INTRA_WEDGE: c_int = 2;
pub const LEFT_DC_PRED: c_int = 3;
pub const LR_HAVE_BOTTOM: c_int = 8;
pub const LR_HAVE_LEFT: c_int = 1;
pub const LR_HAVE_RIGHT: c_int = 2;
pub const LR_HAVE_TOP: c_int = 4;
pub const LR_RESTORE_U: c_int = 2;
pub const LR_RESTORE_V: c_int = 4;
pub const LR_RESTORE_Y: c_int = 1;
pub const MM_OBMC: c_int = 1;
pub const MM_TRANSLATION: c_int = 0;
pub const MM_WARP: c_int = 2;
pub const MV_JOINT_H: c_int = 1;
pub const MV_JOINT_V: c_int = 2;
pub const NEARER_DRL: c_int = 1;
pub const NEARESTMV: c_int = 0;
pub const NEARESTMV_NEARESTMV: c_int = 0;
pub const NEAREST_DRL: c_int = 0;
pub const NEARISH_DRL: c_int = 3;
pub const NEARMV: c_int = 1;
pub const NEAR_DRL: c_int = 2;
pub const NEWMV: c_int = 3;
pub const NEWMV_NEWMV: c_int = 7;
pub const N_BS_SIZES: c_int = 22;
pub const N_COMP_INTER_PRED_MODES: c_int = 8;
pub const N_INTER_INTRA_PRED_MODES: c_int = 4;
pub const N_INTRA_PRED_MODES: c_int = 13;
pub const N_MV_JOINTS: c_int = 4;
pub const N_PARTITIONS: c_int = 10;
pub const N_RECT_TX_SIZES: c_int = 19;
pub const N_SUB8X8_PARTITIONS: c_int = 4;
pub const N_TX_SIZES: c_int = 5;
pub const N_UV_INTRA_PRED_MODES: c_int = 14;
pub const OBU_META_HDR_CLL: c_int = 1;
pub const OBU_META_HDR_MDCV: c_int = 2;
pub const OBU_META_ITUT_T35: c_int = 4;
pub const OBU_META_SCALABILITY: c_int = 3;
pub const OBU_META_TIMECODE: c_int = 5;
pub const PAETH_PRED: c_int = 12;
pub const PARTITION_H: c_int = 1;
pub const PARTITION_H4: c_int = 8;
pub const PARTITION_NONE: c_int = 0;
pub const PARTITION_SPLIT: c_int = 3;
pub const PARTITION_T_BOTTOM_SPLIT: c_int = 5;
pub const PARTITION_T_LEFT_SPLIT: c_int = 6;
pub const PARTITION_T_RIGHT_SPLIT: c_int = 7;
pub const PARTITION_T_TOP_SPLIT: c_int = 4;
pub const PARTITION_V: c_int = 2;
pub const PARTITION_V4: c_int = 9;
pub const PICTURE_FLAG_NEW_OP_PARAMS_INFO: c_int = 2;
pub const PICTURE_FLAG_NEW_SEQUENCE: c_int = 1;
pub const PICTURE_FLAG_NEW_TEMPORAL_UNIT: c_int = 4;
pub const RTX_16X32: c_int = 9;
pub const RTX_16X4: c_int = 14;
pub const RTX_16X64: c_int = 17;
pub const RTX_16X8: c_int = 8;
pub const RTX_32X16: c_int = 10;
pub const RTX_32X64: c_int = 11;
pub const RTX_32X8: c_int = 16;
pub const RTX_4X16: c_int = 13;
pub const RTX_4X8: c_int = 5;
pub const RTX_64X16: c_int = 18;
pub const RTX_64X32: c_int = 12;
pub const RTX_8X16: c_int = 7;
pub const RTX_8X32: c_int = 15;
pub const RTX_8X4: c_int = 6;
pub const SMOOTH_H_PRED: c_int = 11;
pub const SMOOTH_PRED: c_int = 9;
pub const SMOOTH_V_PRED: c_int = 10;
pub const TOP_DC_PRED: c_int = 4;
pub const TX_16X16: c_int = 2;
pub const TX_32X32: c_int = 3;
pub const TX_4X4: c_int = 0;
pub const TX_64X64: c_int = 4;
pub const TX_8X8: c_int = 1;
pub const TX_CLASS_2D: c_int = 0;
pub const TX_CLASS_H: c_int = 1;
pub const TX_CLASS_V: c_int = 2;
pub const VERT_LEFT_PRED: c_int = 8;
pub const VERT_PRED: c_int = 1;
pub const VERT_RIGHT_PRED: c_int = 5;
pub const V_ADST: c_int = 12;
pub const V_DCT: c_int = 10;
pub const V_FLIPADST: c_int = 14;
pub const WHT_WHT: c_int = 16;
pub const Z1_PRED: c_int = 6;
pub const Z2_PRED: c_int = 7;
pub const Z3_PRED: c_int = 8;
pub const __builtin = @import("std").zig.c_translation.builtins;
pub const __helpers = @import("std").zig.c_translation.helpers;
pub const afz_pthread_attr_t = c_int;
pub const afz_pthread_cond_t = c_int;
pub const afz_pthread_mutex_t = c_int;
pub const afz_pthread_once_t = c_int;
pub const afz_pthread_t = c_ulong;
pub const atomic_int = c_int;
pub const atomic_uint = c_uint;
pub const cfl_allowed_mask: c_uint = @bitCast(@as(c_int, (((((((((((((@as(c_int, 1) << @intCast(BS_32x32)) | (@as(c_int, 1) << @intCast(BS_32x16))) | (@as(c_int, 1) << @intCast(BS_32x8))) | (@as(c_int, 1) << @intCast(BS_16x32))) | (@as(c_int, 1) << @intCast(BS_16x16))) | (@as(c_int, 1) << @intCast(BS_16x8))) | (@as(c_int, 1) << @intCast(BS_16x4))) | (@as(c_int, 1) << @intCast(BS_8x32))) | (@as(c_int, 1) << @intCast(BS_8x16))) | (@as(c_int, 1) << @intCast(BS_8x8))) | (@as(c_int, 1) << @intCast(BS_8x4))) | (@as(c_int, 1) << @intCast(BS_4x16))) | (@as(c_int, 1) << @intCast(BS_4x8))) | (@as(c_int, 1) << @intCast(BS_4x4))));
pub const dav1d_memset_pow2_fn = ?*const fn (ptr: ?*anyopaque, value: c_int) callconv(.c) void;
pub const ec_win = usize;
pub const enum_BlockLevel = c_uint;
pub const enum_BlockPartition = c_uint;
pub const enum_BlockSize = c_uint;
pub const enum_CdefEdgeFlags = c_uint;
pub const enum_CompInterPredMode = c_uint;
pub const enum_Dav1dAdaptiveBoolean = c_uint;
pub const enum_Dav1dChromaSamplePosition = c_uint;
pub const enum_Dav1dColorPrimaries = c_uint;
pub const enum_Dav1dDecodeFrameType = c_uint;
pub const enum_Dav1dEventFlags = c_uint;
pub const enum_Dav1dFilterMode = c_uint;
pub const enum_Dav1dFrameType = c_uint;
pub const enum_Dav1dInloopFilterType = c_uint;
pub const enum_Dav1dMatrixCoefficients = c_uint;
pub const enum_Dav1dObuType = c_uint;
pub const enum_Dav1dPixelLayout = c_uint;
pub const enum_Dav1dRestorationType = c_uint;
pub const enum_Dav1dTransferCharacteristics = c_uint;
pub const enum_Dav1dTxfmMode = c_uint;
pub const enum_Dav1dWarpedMotionType = c_uint;
pub const enum_EdgeFlags = c_uint;
pub const enum_Filter2d = c_uint;
pub const enum_InterPredMode = c_uint;
pub const enum_IntraPredMode = c_uint;
pub const enum_LrEdgeFlags = c_uint;
pub const enum_MVJoint = c_uint;
pub const enum_ObuMetaType = c_uint;
pub const enum_PictureFlags = c_uint;
pub const enum_RectTxfmSize = c_uint;
pub const enum_TaskType = c_uint;
pub const enum_TxClass = c_uint;
pub const enum_TxfmType = c_uint;
pub const interintra_allowed_mask: c_uint = @bitCast(@as(c_int, ((((((@as(c_int, 1) << @intCast(BS_32x32)) | (@as(c_int, 1) << @intCast(BS_32x16))) | (@as(c_int, 1) << @intCast(BS_16x32))) | (@as(c_int, 1) << @intCast(BS_16x16))) | (@as(c_int, 1) << @intCast(BS_16x8))) | (@as(c_int, 1) << @intCast(BS_8x16))) | (@as(c_int, 1) << @intCast(BS_8x8))));
pub const pal_idx_finish_fn = ?*const fn (dst: [*c]u8, src: [*c]const u8, bw: c_int, bh: c_int, w: c_int, h: c_int) callconv(.c) void;
pub const ptrdiff_t = c_long;
pub const refmvs_refpair = union_refmvs_refpair;
pub const struct_Av1Filter = extern struct {
    filter_y: [2][32][3][2]u16 = @import("std").mem.zeroes([2][32][3][2]u16),
    filter_uv: [2][32][2][2]u16 = @import("std").mem.zeroes([2][32][2][2]u16),
    cdef_idx: [4]i8 = @import("std").mem.zeroes([4]i8),
    noskip_mask: [16][2]u16 = @import("std").mem.zeroes([16][2]u16),
};
pub const struct_Av1FilterLUT = extern struct {
    e: [64]u8 = @import("std").mem.zeroes([64]u8),
    i: [64]u8 = @import("std").mem.zeroes([64]u8),
    sharp: [2]u64 = @import("std").mem.zeroes([2]u64),
};
pub const struct_Av1Restoration = extern struct {
    lr: [3][4]Av1RestorationUnit = @import("std").mem.zeroes([3][4]Av1RestorationUnit),
};
pub const struct_Av1RestorationUnit = extern struct {
    type: u8 = 0,
    filter_h: [3]i8 = @import("std").mem.zeroes([3]i8),
    filter_v: [3]i8 = @import("std").mem.zeroes([3]i8),
    sgr_weights: [2]i8 = @import("std").mem.zeroes([2]i8),
};
pub const struct_CdfCoefContext = extern struct {
    eob_bin_16: [2][2][8]u16 align(32) = @import("std").mem.zeroes([2][2][8]u16),
    eob_bin_32: [2][2][8]u16 = @import("std").mem.zeroes([2][2][8]u16),
    eob_bin_64: [2][2][8]u16 = @import("std").mem.zeroes([2][2][8]u16),
    eob_bin_128: [2][2][8]u16 = @import("std").mem.zeroes([2][2][8]u16),
    eob_bin_256: [2][2][16]u16 = @import("std").mem.zeroes([2][2][16]u16),
    eob_bin_512: [2][16]u16 = @import("std").mem.zeroes([2][16]u16),
    eob_bin_1024: [2][16]u16 = @import("std").mem.zeroes([2][16]u16),
    eob_base_tok: [5][2][4][4]u16 = @import("std").mem.zeroes([5][2][4][4]u16),
    base_tok: [5][2][41][4]u16 = @import("std").mem.zeroes([5][2][41][4]u16),
    br_tok: [4][2][21][4]u16 = @import("std").mem.zeroes([4][2][21][4]u16),
    eob_hi_bit: [5][2][11][2]u16 = @import("std").mem.zeroes([5][2][11][2]u16),
    skip: [5][13][2]u16 = @import("std").mem.zeroes([5][13][2]u16),
    dc_sign: [2][3][2]u16 = @import("std").mem.zeroes([2][3][2]u16),
};
pub const struct_CdfMvComponent = extern struct {
    classes: [16]u16 align(32) = @import("std").mem.zeroes([16]u16),
    sign: [2]u16 = @import("std").mem.zeroes([2]u16),
    class0: [2]u16 = @import("std").mem.zeroes([2]u16),
    class0_fp: [2][4]u16 = @import("std").mem.zeroes([2][4]u16),
    class0_hp: [2]u16 = @import("std").mem.zeroes([2]u16),
    classN: [10][2]u16 = @import("std").mem.zeroes([10][2]u16),
    classN_fp: [4]u16 align(32) = @import("std").mem.zeroes([4]u16),
    classN_hp: [2]u16 = @import("std").mem.zeroes([2]u16),
};
pub const struct_CdfMvContext = extern struct {
    comp: [2]CdfMvComponent = @import("std").mem.zeroes([2]CdfMvComponent),
    joint: [4]u16 = @import("std").mem.zeroes([4]u16),
};
pub const struct_Dav1dContentLightLevel = extern struct {
    max_content_light_level: u16 = 0,
    max_frame_average_light_level: u16 = 0,
};
pub const struct_Dav1dFilmGrainData = extern struct {
    seed: c_uint = 0,
    num_y_points: c_int = 0,
    y_points: [14][2]u8 = @import("std").mem.zeroes([14][2]u8),
    chroma_scaling_from_luma: c_int = 0,
    num_uv_points: [2]c_int = @import("std").mem.zeroes([2]c_int),
    uv_points: [2][10][2]u8 = @import("std").mem.zeroes([2][10][2]u8),
    scaling_shift: c_int = 0,
    ar_coeff_lag: c_int = 0,
    ar_coeffs_y: [24]i8 = @import("std").mem.zeroes([24]i8),
    ar_coeffs_uv: [2][28]i8 = @import("std").mem.zeroes([2][28]i8),
    ar_coeff_shift: u64 = 0,
    grain_scale_shift: c_int = 0,
    uv_mult: [2]c_int = @import("std").mem.zeroes([2]c_int),
    uv_luma_mult: [2]c_int = @import("std").mem.zeroes([2]c_int),
    uv_offset: [2]c_int = @import("std").mem.zeroes([2]c_int),
    overlap_flag: c_int = 0,
    clip_to_restricted_range: c_int = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_4 = extern struct {
    buffer_removal_time: u32 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_5 = extern struct {
    buffer_removal_time: u32 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_6 = extern struct {
    buffer_removal_time: u32 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_7 = extern struct {
    buffer_removal_time: u32 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_8 = extern struct {
    buffer_removal_time: u32 = 0,
};
pub const struct_Dav1dITUTT35 = extern struct {
    country_code: u8 = 0,
    country_code_extension_byte: u8 = 0,
    payload_size: usize = 0,
    payload: [*c]u8 = null,
};
pub const struct_Dav1dLoopfilterModeRefDeltas = extern struct {
    mode_delta: [2]i8 = @import("std").mem.zeroes([2]i8),
    ref_delta: [8]i8 = @import("std").mem.zeroes([8]i8),
};
pub const struct_Dav1dMasteringDisplay = extern struct {
    primaries: [3][2]u16 = @import("std").mem.zeroes([3][2]u16),
    white_point: [2]u16 = @import("std").mem.zeroes([2]u16),
    max_luminance: u32 = 0,
    min_luminance: u32 = 0,
};
pub const struct_Dav1dMemPoolBuffer = extern struct {
    data: ?*anyopaque = null,
    next: [*c]struct_Dav1dMemPoolBuffer = null,
};
pub const struct_Dav1dPalDSPContext = extern struct {
    pal_idx_finish: pal_idx_finish_fn = null,
};
pub const struct_Dav1dPictureParameters = extern struct {
    w: c_int = 0,
    h: c_int = 0,
    layout: enum_Dav1dPixelLayout = @import("std").mem.zeroes(enum_Dav1dPixelLayout),
    bpc: c_int = 0,
};
pub const struct_Dav1dRef = extern struct {
    data: ?*anyopaque = null,
    const_data: ?*const anyopaque = null,
    ref_cnt: atomic_int = 0,
    free_ref: c_int = 0,
    free_callback: ?*const fn (data: [*c]const u8, user_data: ?*anyopaque) callconv(.c) void = null,
    user_data: ?*anyopaque = null,
};
pub const struct_Dav1dSegmentationData = extern struct {
    delta_q: i16 = 0,
    delta_lf_y_v: i8 = 0,
    delta_lf_y_h: i8 = 0,
    delta_lf_u: i8 = 0,
    delta_lf_v: i8 = 0,
    ref: i8 = 0,
    skip: u8 = 0,
    globalmv: u8 = 0,
};
pub const struct_Dav1dSegmentationDataSet = extern struct {
    d: [8]Dav1dSegmentationData = @import("std").mem.zeroes([8]Dav1dSegmentationData),
    preskip: u8 = 0,
    last_active_segid: i8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_2 = extern struct {
    decoder_buffer_delay: u32 = 0,
    encoder_buffer_delay: u32 = 0,
    low_delay_mode: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_3 = extern struct {
    decoder_buffer_delay: u32 = 0,
    encoder_buffer_delay: u32 = 0,
    low_delay_mode: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_4 = extern struct {
    decoder_buffer_delay: u32 = 0,
    encoder_buffer_delay: u32 = 0,
    low_delay_mode: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_5 = extern struct {
    decoder_buffer_delay: u32 = 0,
    encoder_buffer_delay: u32 = 0,
    low_delay_mode: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_6 = extern struct {
    decoder_buffer_delay: u32 = 0,
    encoder_buffer_delay: u32 = 0,
    low_delay_mode: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingPoint_1 = extern struct {
    major_level: u8 = 0,
    minor_level: u8 = 0,
    initial_display_delay: u8 = 0,
    idc: u16 = 0,
    tier: u8 = 0,
    decoder_model_param_present: u8 = 0,
    display_model_param_present: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingPoint_2 = extern struct {
    major_level: u8 = 0,
    minor_level: u8 = 0,
    initial_display_delay: u8 = 0,
    idc: u16 = 0,
    tier: u8 = 0,
    decoder_model_param_present: u8 = 0,
    display_model_param_present: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingPoint_3 = extern struct {
    major_level: u8 = 0,
    minor_level: u8 = 0,
    initial_display_delay: u8 = 0,
    idc: u16 = 0,
    tier: u8 = 0,
    decoder_model_param_present: u8 = 0,
    display_model_param_present: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingPoint_4 = extern struct {
    major_level: u8 = 0,
    minor_level: u8 = 0,
    initial_display_delay: u8 = 0,
    idc: u16 = 0,
    tier: u8 = 0,
    decoder_model_param_present: u8 = 0,
    display_model_param_present: u8 = 0,
};
pub const struct_Dav1dSequenceHeaderOperatingPoint_5 = extern struct {
    major_level: u8 = 0,
    minor_level: u8 = 0,
    initial_display_delay: u8 = 0,
    idc: u16 = 0,
    tier: u8 = 0,
    decoder_model_param_present: u8 = 0,
    display_model_param_present: u8 = 0,
};
pub const struct_FrameTileThreadData_45 = extern struct {
    lowest_pixel_mem: [*c][7][2]c_int = null,
    lowest_pixel_mem_sz: c_int = 0,
};
pub const struct_FrameTileThreadData_46 = extern struct {
    lowest_pixel_mem: [*c][7][2]c_int = null,
    lowest_pixel_mem_sz: c_int = 0,
};
pub const struct_ScalableMotionParams_19 = extern struct {
    scale: c_int = 0,
    step: c_int = 0,
};
pub const struct_ScalableMotionParams_20 = extern struct {
    scale: c_int = 0,
    step: c_int = 0,
};
pub const struct___va_list_1 = extern struct {
    unnamed_0: ?*anyopaque = null,
    unnamed_1: ?*anyopaque = null,
    unnamed_2: ?*anyopaque = null,
    unnamed_3: c_int = 0,
    unnamed_4: c_int = 0,
};
pub const struct_thread_data = extern struct {
    thread: afz_pthread_t = 0,
    cond: afz_pthread_cond_t = 0,
    lock: afz_pthread_mutex_t = 0,
    inited: c_int = 0,
};
pub const union_alias16 = extern union {
    u16: u16,
    u8: [2]u8,
};
pub const union_alias32 = extern union {
    u32: u32,
    u8: [4]u8,
};
pub const union_alias64 = extern union {
    u64: u64,
    u8: [8]u8,
};
pub const union_alias8 = extern union {
    u8: u8,
};
pub const union_refmvs_refpair = extern union {
    ref: [2]i8 align(1),
    pair: u16 align(1),
};
pub const wedge_allowed_mask: c_uint = @bitCast(@as(c_int, ((((((((@as(c_int, 1) << @intCast(BS_32x32)) | (@as(c_int, 1) << @intCast(BS_32x16))) | (@as(c_int, 1) << @intCast(BS_32x8))) | (@as(c_int, 1) << @intCast(BS_16x32))) | (@as(c_int, 1) << @intCast(BS_16x16))) | (@as(c_int, 1) << @intCast(BS_16x8))) | (@as(c_int, 1) << @intCast(BS_8x32))) | (@as(c_int, 1) << @intCast(BS_8x16))) | (@as(c_int, 1) << @intCast(BS_8x8))));
