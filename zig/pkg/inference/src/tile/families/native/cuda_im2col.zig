// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Existing Tile im2col body, static layout/stride specializations. No model
//! numerical implementation or runtime layout branch is introduced.
const tile = @import("antfly_tile");
pub const source_0 =
    "struct antfly_tile_im2col_params { unsigned int channels; unsigned int t_in; unsigned int t_out; };\n" ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_nct_k3_s1_p1_f32", .kernel = 3, .stride = 1, .pad = 1, .layout = .channel_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_ntc_k3_s1_p1_f32", .kernel = 3, .stride = 1, .pad = 1, .layout = .time_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_nct_k3_s2_p1_f32", .kernel = 3, .stride = 2, .pad = 1, .layout = .channel_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_ntc_k3_s2_p1_f32", .kernel = 3, .stride = 2, .pad = 1, .layout = .time_major }, .cuda);
