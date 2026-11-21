// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Existing Tile im2col body, static layout/stride specializations. No model
//! numerical implementation or runtime layout branch is introduced.
const tile = @import("antfly_tile");
pub const source_0 =
    "struct antfly_tile_im2col_params { unsigned int channels; unsigned int t_in; unsigned int t_out; };\n" ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_nct_k3_s1_p1_f32", .kernel = 3, .stride = 1, .pad = 1, .layout = .channel_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_ntc_k3_s1_p1_f32", .kernel = 3, .stride = 1, .pad = 1, .layout = .time_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_nct_k3_s2_p1_f32", .kernel = 3, .stride = 2, .pad = 1, .layout = .channel_major }, .cuda) ++
    tile.gpu_model.renderIm2col(.{ .name = "termite_im2col_ntc_k3_s2_p1_f32", .kernel = 3, .stride = 2, .pad = 1, .layout = .time_major }, .cuda);
